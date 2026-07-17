/**
 * @file sample_buffer.h
 * @brief Local flash ring buffer for rv_feature_state_t samples + HTTP pull API.
 *
 * Persists sensing output to the (previously unused) `spiffs`-labeled data
 * partition so readings survive even when nothing is listening on the
 * network for stream_sender's live UDP stream. Registers a read-only HTTP
 * API on the existing OTA/WASM httpd server:
 *
 *   GET /data/status — {oldest_ts, newest_ts, record_count, buffer_full}
 *   GET /data/pull?since=<unix_seconds>&limit=<n> — newest data since a marker
 *
 * Records only carry a meaningful `utc_ts` once SNTP has synced (see
 * main.c's IP_EVENT_STA_GOT_IP handler) — before that, records are stored
 * with utc_ts=0 and are not resolvable by /data/pull's `since` filter.
 */

#ifndef SAMPLE_BUFFER_H
#define SAMPLE_BUFFER_H

#include "esp_err.h"
#include "esp_http_server.h"
#include "rv_feature_state.h"

/**
 * Mount the ring buffer over the `spiffs` data partition. Scans the
 * partition once to recover the write cursor (no persisted header —
 * see sample_buffer.c for why). Must be called once at boot before
 * sample_buffer_append().
 */
esp_err_t sample_buffer_start(void);

/**
 * Append one feature-state record to the ring buffer, stamped with the
 * current wall-clock time (0 if SNTP hasn't synced yet). Best-effort and
 * independent of whether the same packet was also sent live over UDP —
 * this is the point of the buffer. Safe to call even if
 * sample_buffer_start() failed (no-ops).
 */
void sample_buffer_append(const rv_feature_state_t *pkt);

/**
 * Register the /data/status and /data/pull HTTP handlers on an existing
 * httpd server (e.g. the OTA server from ota_update_init_ex()).
 */
esp_err_t sample_buffer_http_register(httpd_handle_t server);

#endif /* SAMPLE_BUFFER_H */
