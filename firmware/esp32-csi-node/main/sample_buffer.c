/**
 * @file sample_buffer.c
 * @brief Local flash ring buffer for rv_feature_state_t samples + HTTP pull API.
 *
 * Layout: the `spiffs`-labeled data partition (partitions_display.csv,
 * 0x420000, 0x1E0000 = 1.875 MB) is unmounted/unused everywhere else in
 * this firmware, so we repurpose its raw address range directly via
 * esp_partition_read/write/erase_range — no filesystem, no new managed
 * component.
 *
 * The partition is divided into fixed 4096-byte erase sectors, each
 * holding RECORDS_PER_SECTOR fixed-size records written sequentially.
 * Once a sector is full, the next sector is erased and filling continues
 * there; on reaching the last sector, we wrap back to sector 0 (erasing
 * it, silently discarding its old contents — this is a ring buffer, not
 * a log).
 *
 * No cursor/header is persisted to flash. Erasing a dedicated header
 * sector on every sector transition (~once/minute at 1 Hz) would rack up
 * tens of thousands of erase cycles per year of nightly use — meaningful
 * wear for a single sector. Instead, sample_buffer_start() recovers the
 * write cursor by scanning: every sector's first-record timestamp is
 * read once (cheap — a few hundred 4-byte reads), which is enough to
 * find the sector with the newest first-record timestamp; that sector is
 * then scanned record-by-record (at most RECORDS_PER_SECTOR reads) to
 * find its exact fill level. This costs a bit of boot time and zero
 * extra flash wear.
 *
 * Known limitation: sector erase (~tens of ms) happens synchronously
 * inside sample_buffer_append(), called from the adaptive controller's
 * 1 Hz emit path — this can add a one-time-per-minute hiccup to that
 * timer callback. Acceptable for MVP; a future pass could defer erasure
 * to a background task via a queue.
 */

#include "sample_buffer.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#include "esp_log.h"
#include "esp_partition.h"
#include "nvs.h"
#include "nvs_flash.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

static const char *TAG = "sample_buffer";

#define SAMPLE_BUFFER_PARTITION_LABEL "spiffs"
#define SECTOR_SIZE 4096u

/* One-time-format marker. The spiffs-labeled partition is unused by any
 * *code path* today, but that doesn't mean it's blank flash — this board
 * has been through factory test firmware and earlier RuView development
 * before we got it, and the region may hold leftover non-erased bytes
 * that happen to not equal the 0xFFFFFFFF "erased" marker our recovery
 * scan relies on. A single NVS flag (one write, ever — not per-record,
 * no wear concern) lets us force a full-partition erase exactly once per
 * partition layout, so recover_cursor() can trust "0xFFFFFFFF means never
 * written" from then on.
 *
 * The key is derived from the partition's size rather than a fixed
 * string: whenever partitions_display.csv grows/shrinks this partition
 * (as it just did, 0x1E0000 -> 0x3E0000), the old key silently stops
 * matching and a fresh full erase runs automatically — otherwise the
 * newly-exposed region would keep whatever leftover bytes were already
 * there, reintroducing the exact garbage-data bug this was built to fix.
 */
#define FORMAT_NVS_NAMESPACE "sample_buf"

typedef struct __attribute__((packed)) {
    uint32_t utc_ts;           /* Seconds since epoch (wall clock), 0 if unsynced. */
    rv_feature_state_t pkt;    /* 60 bytes. */
} sample_record_t;

#define RECORD_SIZE (sizeof(sample_record_t))
#define RECORDS_PER_SECTOR (SECTOR_SIZE / RECORD_SIZE)
#define ERASED_TS 0xFFFFFFFFu

_Static_assert(sizeof(rv_feature_state_t) == 60, "unexpected rv_feature_state_t size");
_Static_assert(RECORD_SIZE == 64, "sample_record_t expected to be 64 bytes");
_Static_assert(SECTOR_SIZE % RECORD_SIZE == 0, "records must divide evenly into a sector");

static const esp_partition_t *s_part = NULL;
static uint32_t s_num_sectors = 0;
static uint32_t s_write_sector = 0;          /* Sector currently being filled. */
static uint32_t s_write_offset = 0;          /* Next free record slot within s_write_sector. */
static bool s_wrapped = false;               /* Has the ring gone all the way around at least once? */
static bool s_ready = false;
static SemaphoreHandle_t s_lock = NULL;

static esp_err_t read_record(uint32_t sector, uint32_t offset, sample_record_t *out)
{
    size_t addr = (size_t)sector * SECTOR_SIZE + (size_t)offset * RECORD_SIZE;
    return esp_partition_read(s_part, addr, out, RECORD_SIZE);
}

static esp_err_t write_record(uint32_t sector, uint32_t offset, const sample_record_t *rec)
{
    size_t addr = (size_t)sector * SECTOR_SIZE + (size_t)offset * RECORD_SIZE;
    return esp_partition_write(s_part, addr, rec, RECORD_SIZE);
}

/** Find how many leading records in `sector` are filled (non-erased). */
static uint32_t scan_sector_fill(uint32_t sector)
{
    sample_record_t rec;
    for (uint32_t i = 0; i < RECORDS_PER_SECTOR; i++) {
        if (read_record(sector, i, &rec) != ESP_OK || rec.utc_ts == ERASED_TS) {
            return i;
        }
    }
    return RECORDS_PER_SECTOR;
}

/**
 * Erase the whole buffer partition exactly once per firmware image (see
 * FORMAT_NVS_KEY comment above). No-op on every boot after the first.
 */
static esp_err_t ensure_partition_formatted(void)
{
    /* NVS keys are capped at 15 chars; a hex size fits comfortably
     * ("fmt_3e0000" = 10 chars) and stays unique per distinct layout. */
    char key[16];
    snprintf(key, sizeof(key), "fmt_%lx", (unsigned long)s_part->size);

    nvs_handle_t nvs;
    esp_err_t err = nvs_open(FORMAT_NVS_NAMESPACE, NVS_READWRITE, &nvs);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "nvs_open failed: %s — formatting unconditionally to be safe",
                 esp_err_to_name(err));
    } else {
        uint8_t formatted = 0;
        esp_err_t get_err = nvs_get_u8(nvs, key, &formatted);
        if (get_err == ESP_OK && formatted) {
            nvs_close(nvs);
            return ESP_OK; /* Already formatted at this partition size. */
        }
    }

    ESP_LOGI(TAG, "first boot at this partition size (%lu bytes) — erasing "
                  "(clears any leftover pre-existing flash contents)...",
             (unsigned long)s_part->size);
    esp_err_t erase_err = esp_partition_erase_range(s_part, 0, s_part->size);
    if (erase_err != ESP_OK) {
        if (err == ESP_OK) nvs_close(nvs);
        return erase_err;
    }
    ESP_LOGI(TAG, "partition erase complete");

    if (err == ESP_OK) {
        nvs_set_u8(nvs, key, 1);
        nvs_commit(nvs);
        nvs_close(nvs);
    }
    return ESP_OK;
}

/**
 * Recover the write cursor by scanning every sector's first-record
 * timestamp. See file header comment for the algorithm.
 */
static esp_err_t recover_cursor(void)
{
    sample_record_t rec;
    int32_t best_sector = -1;
    uint32_t best_ts = 0;
    bool any_filled = false;
    bool all_filled = true;

    for (uint32_t s = 0; s < s_num_sectors; s++) {
        esp_err_t rerr = read_record(s, 0, &rec);
        bool filled = (rerr == ESP_OK) && (rec.utc_ts != ERASED_TS);
        if (filled) {
            any_filled = true;
            /* First pass, or a strictly newer first-record timestamp —
             * ties (e.g. clock not synced, utc_ts=0 everywhere) keep the
             * lowest index so we default to sector 0 predictably. */
            if (best_sector < 0 || rec.utc_ts > best_ts) {
                best_sector = (int32_t)s;
                best_ts = rec.utc_ts;
            }
        } else {
            all_filled = false;
        }
    }

    if (!any_filled) {
        s_write_sector = 0;
        s_write_offset = 0;
        s_wrapped = false;
        ESP_LOGI(TAG, "buffer empty, starting at sector 0");
        return ESP_OK;
    }

    /* Wrapped iff every sector's first record is filled. Note this must
     * be decided from the single scan above, not assumed from whichever
     * branch best_sector falls into below — e.g. sector 0 finishing
     * exactly full on the very first lap looks identical, at the
     * best_sector level, to a genuinely wrapped ring; only the
     * all-sectors check tells them apart. */
    s_wrapped = all_filled;

    uint32_t fill = scan_sector_fill((uint32_t)best_sector);
    if (fill >= RECORDS_PER_SECTOR) {
        /* Newest sector is completely full — move on to the next one. */
        s_write_sector = ((uint32_t)best_sector + 1) % s_num_sectors;
        s_write_offset = 0;
    } else {
        s_write_sector = (uint32_t)best_sector;
        s_write_offset = fill;
    }

    ESP_LOGI(TAG, "recovered cursor: sector=%lu offset=%lu wrapped=%d",
             (unsigned long)s_write_sector, (unsigned long)s_write_offset, (int)s_wrapped);
    return ESP_OK;
}

esp_err_t sample_buffer_start(void)
{
    s_part = esp_partition_find_first(ESP_PARTITION_TYPE_DATA,
                                       ESP_PARTITION_SUBTYPE_DATA_SPIFFS,
                                       SAMPLE_BUFFER_PARTITION_LABEL);
    if (s_part == NULL) {
        ESP_LOGW(TAG, "partition '%s' not found — local buffering disabled",
                 SAMPLE_BUFFER_PARTITION_LABEL);
        return ESP_ERR_NOT_FOUND;
    }

    s_num_sectors = s_part->size / SECTOR_SIZE;
    if (s_num_sectors == 0) {
        ESP_LOGW(TAG, "partition too small for even one sector — local buffering disabled");
        return ESP_ERR_INVALID_SIZE;
    }

    s_lock = xSemaphoreCreateMutex();
    if (s_lock == NULL) {
        ESP_LOGE(TAG, "failed to create mutex");
        return ESP_ERR_NO_MEM;
    }

    esp_err_t err = ensure_partition_formatted();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "format check/erase failed: %s — local buffering disabled",
                 esp_err_to_name(err));
        return err;
    }

    err = recover_cursor();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "cursor recovery failed: %s — local buffering disabled",
                 esp_err_to_name(err));
        return err;
    }

    uint32_t capacity = s_num_sectors * RECORDS_PER_SECTOR;
    ESP_LOGI(TAG, "local buffer ready: %lu sectors, capacity %lu records (~%.1f h at 1 Hz)",
             (unsigned long)s_num_sectors, (unsigned long)capacity,
             (double)capacity / 3600.0);

    s_ready = true;
    return ESP_OK;
}

void sample_buffer_append(const rv_feature_state_t *pkt)
{
    if (!s_ready || pkt == NULL) return;
    if (xSemaphoreTake(s_lock, pdMS_TO_TICKS(100)) != pdTRUE) {
        ESP_LOGW(TAG, "append: lock timeout, dropping record");
        return;
    }

    if (s_write_offset == 0) {
        /* Entering a fresh sector — erase it first (NOR flash requires
         * erase-before-write). Silently discards whatever old data was
         * here; that's the ring-buffer contract. */
        esp_err_t erase_err = esp_partition_erase_range(s_part,
                                                          (size_t)s_write_sector * SECTOR_SIZE,
                                                          SECTOR_SIZE);
        if (erase_err != ESP_OK) {
            ESP_LOGW(TAG, "sector erase failed: %s", esp_err_to_name(erase_err));
            xSemaphoreGive(s_lock);
            return;
        }
    }

    sample_record_t rec;
    memset(&rec, 0, sizeof(rec));
    time_t now = time(NULL);
    /* Before SNTP has synced, time(NULL) returns a small epoch value
     * (seconds since boot-ish, well before 2020) — store 0 so the HTTP
     * pull side can tell "unsynced" apart from a genuine old timestamp. */
    rec.utc_ts = (now > 1600000000) ? (uint32_t)now : 0;
    memcpy(&rec.pkt, pkt, sizeof(rec.pkt));

    esp_err_t err = write_record(s_write_sector, s_write_offset, &rec);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "record write failed: %s", esp_err_to_name(err));
        xSemaphoreGive(s_lock);
        return;
    }

    s_write_offset++;
    if (s_write_offset >= RECORDS_PER_SECTOR) {
        s_write_offset = 0;
        s_write_sector = (s_write_sector + 1) % s_num_sectors;
        if (s_write_sector == 0) s_wrapped = true;
    }

    xSemaphoreGive(s_lock);
}

/** Oldest valid slot in ring order (wrapping), or {0,0} if buffer is empty/unwrapped. */
static void oldest_slot(uint32_t *out_sector, uint32_t *out_offset)
{
    if (!s_wrapped) {
        *out_sector = 0;
        *out_offset = 0;
    } else {
        *out_sector = s_write_sector;
        *out_offset = s_write_offset;
    }
}

static bool slot_advance(uint32_t *sector, uint32_t *offset)
{
    (*offset)++;
    if (*offset >= RECORDS_PER_SECTOR) {
        *offset = 0;
        *sector = (*sector + 1) % s_num_sectors;
    }
    /* Stop once we've caught up to the write cursor. */
    return !(*sector == s_write_sector && *offset == s_write_offset);
}

static esp_err_t data_status_handler(httpd_req_t *req)
{
    if (!s_ready) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "buffer not ready");
        return ESP_FAIL;
    }

    xSemaphoreTake(s_lock, portMAX_DELAY);
    uint32_t osec, ooff;
    oldest_slot(&osec, &ooff);
    bool has_data = s_wrapped || !(s_write_sector == 0 && s_write_offset == 0);

    uint32_t oldest_ts = 0, newest_ts = 0, count = 0;
    if (has_data) {
        sample_record_t rec;
        if (read_record(osec, ooff, &rec) == ESP_OK) oldest_ts = rec.utc_ts;

        uint32_t last_sector = s_write_sector;
        uint32_t last_offset = s_write_offset;
        if (last_offset == 0) {
            last_sector = (last_sector + s_num_sectors - 1) % s_num_sectors;
            last_offset = RECORDS_PER_SECTOR - 1;
        } else {
            last_offset -= 1;
        }
        if (read_record(last_sector, last_offset, &rec) == ESP_OK) newest_ts = rec.utc_ts;

        count = s_wrapped
            ? s_num_sectors * RECORDS_PER_SECTOR
            : (s_write_sector * RECORDS_PER_SECTOR + s_write_offset);
    }
    xSemaphoreGive(s_lock);

    char response[192];
    int len = snprintf(response, sizeof(response),
        "{\"oldest_ts\":%lu,\"newest_ts\":%lu,\"record_count\":%lu,\"buffer_full\":%s}",
        (unsigned long)oldest_ts, (unsigned long)newest_ts, (unsigned long)count,
        s_wrapped ? "true" : "false");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, response, len);
    return ESP_OK;
}

static uint32_t query_uint(httpd_req_t *req, const char *key, uint32_t default_val)
{
    char buf[64];
    size_t buf_len = httpd_req_get_url_query_len(req) + 1;
    if (buf_len <= 1 || buf_len > sizeof(buf)) return default_val;
    if (httpd_req_get_url_query_str(req, buf, buf_len) != ESP_OK) return default_val;

    char val[32];
    if (httpd_query_key_value(buf, key, val, sizeof(val)) != ESP_OK) return default_val;
    return (uint32_t)strtoul(val, NULL, 10);
}

/* Cap the number of records per response so one slow client can't hog
 * the httpd worker or blow a stack buffer. NightPlug's sync command
 * loops, calling again with an advanced `since` until caught up. */
#define PULL_MAX_LIMIT 200u

static esp_err_t data_pull_handler(httpd_req_t *req)
{
    if (!s_ready) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "buffer not ready");
        return ESP_FAIL;
    }

    uint32_t since = query_uint(req, "since", 0);
    uint32_t limit = query_uint(req, "limit", PULL_MAX_LIMIT);
    if (limit == 0 || limit > PULL_MAX_LIMIT) limit = PULL_MAX_LIMIT;

    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr_chunk(req, "{\"records\":[");

    xSemaphoreTake(s_lock, portMAX_DELAY);
    uint32_t sector, offset;
    oldest_slot(&sector, &offset);
    bool has_data = s_wrapped || !(s_write_sector == 0 && s_write_offset == 0);

    uint32_t emitted = 0;
    bool first = true;
    char chunk[160];
    if (has_data) {
        do {
            sample_record_t rec;
            if (read_record(sector, offset, &rec) == ESP_OK &&
                rec.utc_ts != ERASED_TS && rec.utc_ts > since) {
                int len = snprintf(chunk, sizeof(chunk),
                    "%s{\"ts\":%lu,\"presence\":%.3f,\"motion\":%.3f,"
                    "\"respiration_bpm\":%.2f,\"respiration_conf\":%.3f,"
                    "\"quality_flags\":%u}",
                    first ? "" : ",",
                    (unsigned long)rec.utc_ts,
                    (double)rec.pkt.presence_score,
                    (double)rec.pkt.motion_score,
                    (double)rec.pkt.respiration_bpm,
                    (double)rec.pkt.respiration_conf,
                    (unsigned)rec.pkt.quality_flags);
                httpd_resp_send_chunk(req, chunk, len);
                first = false;
                emitted++;
                if (emitted >= limit) break;
            }
        } while (slot_advance(&sector, &offset));
    }
    xSemaphoreGive(s_lock);

    char tail[48];
    int tail_len = snprintf(tail, sizeof(tail), "],\"count\":%lu}", (unsigned long)emitted);
    httpd_resp_send_chunk(req, tail, tail_len);
    httpd_resp_send_chunk(req, NULL, 0); /* End chunked response. */
    return ESP_OK;
}

esp_err_t sample_buffer_http_register(httpd_handle_t server)
{
    if (server == NULL) return ESP_ERR_INVALID_ARG;

    httpd_uri_t status_uri = {
        .uri      = "/data/status",
        .method   = HTTP_GET,
        .handler  = data_status_handler,
        .user_ctx = NULL,
    };
    httpd_register_uri_handler(server, &status_uri);

    httpd_uri_t pull_uri = {
        .uri      = "/data/pull",
        .method   = HTTP_GET,
        .handler  = data_pull_handler,
        .user_ctx = NULL,
    };
    httpd_register_uri_handler(server, &pull_uri);

    ESP_LOGI(TAG, "local buffer HTTP endpoints registered:");
    ESP_LOGI(TAG, "  GET /data/status — buffer coverage summary");
    ESP_LOGI(TAG, "  GET /data/pull?since=<unix_s>&limit=<n> — pull buffered records");
    return ESP_OK;
}
