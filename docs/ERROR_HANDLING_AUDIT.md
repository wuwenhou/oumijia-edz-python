# OUMIJIA — FULL ERROR HANDLING SYSTEM AUDIT

Ngày audit: 2026-11-17 · Phạm vi: production **read-only**, staging fixed & tested.
Không restart, không đổi DB, không retry, không đăng file thử.

## 1. Trạng thái hệ thống hiện tại (production)

| Mục | Giá trị |
|---|---|
| Service | `edz-telegram.service` **active/running**, PID **4051500**, NRestarts **0** |
| Publisher | V2 `PUBLISHER_MODE=normal`, concurrency **1/1**, canary OFF, archive stream false |
| Rust service | inactive |
| Fault lock | `upload_engine_fault` = **None** (không khóa) |
| FloodWait | Job `tech.035400805.edz` held với `FloodWait 1634s`; cooldown còn ~647s tại thời điểm audit — **đã tôn trọng**, không bỏ qua |
| SQLite | `journal_mode=wal`, `quick_check=ok` |
| Watchdog timer | `edz-server-watchdog.timer` **inactive** ⚠️ |
| Journal 24h | 310 dòng error/exception/flood/warning; chủ yếu là reconciliation lặp lỗi `scan_limit=25000` |

### Bảng đếm (read-only)

| Bảng | Tổng | Chi tiết |
|---|---:|---|
| messages | 1,335 | complete 995 · held 322 · staged 18 |
| artifacts | 139,788 | posted 134,178 · ready 5,304 · held 306 |
| upload_jobs | 6,535 | queued 5,302 · posted 1,231 · **held 2** |
| upload_events | 7,768 | enqueued 6,535 · ack 1,231 · **ambiguous 2** |
| auto_alerts | 1,828 | **sent 1,828**, không có `sending`/`pending_retry` kẹt |
| auto_issues | 1 | `intake: database is locked` (cũ, chưa có cờ resolve) |

- Job `posting` tồn tại >300s: **không có**.
- 2 job held V2: `tech.1272001H9.edz` (protected chat, canary 1) và
  `tech.035400805.edz` (FloodWait 1634s, cần đối soát).

## 2. Luồng xử lý lỗi — đánh giá

| # | Trường hợp | Xử lý hiện tại | Retry | Mất dữ liệu | Trùng |
|---|---|---|---|---|---|
| 1 | FloodWait + cooldown | `set_cooldown` + held + sleep đúng giây; cooldown dùng chung V1/V2 | Không tự retry | Không | Không |
| 2 | Timeout / mất mạng / mơ hồ | `hold(ambiguous=True)`, event `ambiguous`, không claim lại | **Không** | Không | Không |
| 3 | Telegram ACK ok, SQLite ACK fail | `ack()` rollback 1 transaction; job quay lại `posting`→ trên restart thành `held` | Không tự | Có rủi ro "đã đăng nhưng chưa ACK" → cần đối soát | Không tự |
| 4 | SQLite locked / I/O / disk full / corruption | `sqlite3.Error` → `mark_fault` ghi cờ bền vững, publisher khóa qua restart | Không | Không (fail-closed) | Không |
| 5 | EDZ thiếu/sai size/sai SHA | held ở pre-send, ghi event `error` | Giới hạn (pre-send) | Không | Không |
| 6 | 7z thiếu phần/CRC lỗi | CRC guard abort **trước** khi gửi byte; demux theo toàn bộ member | Không | Không | Không |
| 7 | Crash/restart khi upload | `recover_posting()` → held, không auto-resend | Không | Không | Không |
| 8 | Cùng tên (khác SHA) | `upload_name_owners` atomic, ACK name-match guard | Không | Không | **Chặn** |
| 9 | Job held V2 vắng mặt ở báo cáo lỗi | **Có lỗi** — `query_held()` và `auto_alerts.next_alert()` chỉ đọc `messages` + `artifacts` | — | — | — |
| 10 | Control Bot / watchdog không gửi được alert | `auto_alerts` reserve-before-send, retry `pending_retry`; watchdog queue trên đĩa. Nhưng timer đang **inactive** | Có (alert) | Không | Không |

## 3. Thông báo — đối chiếu source

- `auto_alerts.next_alert()` quét `messages` held + `artifacts` held + `auto_issues`.
  **Không quét `upload_jobs`** → 2 job held V2 (trong đó có FloodWait) **không được báo**
  tới Control Bot trên production.
- `review_reports.query_held()` (dùng cho `/errors`, `/errors csv`, `/errors files`) cũng
  chỉ quét `messages` + `artifacts`. Kiểm tra thực tế: `query_held` trả **628 dòng**,
  cả 2 tên V2 held đều `IN_REPORT=False`.
- `/errors` không phân biệt rõ "lỗi đã biết / lỗi mới / cần admin" ngoài category.
- Alert đã gửi có trạng thái `sent` đầy đủ; cơ chế reserve (`mark_alert_sending`)
  chống lặp khi mất mạng hoạt động đúng.
- Khi lỗi hết hiệu lực: `watchdog.evaluate` có thông báo **"ĐÃ PHỤC HỒI"** cho lỗi server;
  nhưng `auto_issues` **không có cờ resolved** → issue cũ vẫn được phát alert mãi (dù key
  dedup nên chỉ 1 lần) và không có thông báo resolved.

### Đã sửa trên staging (chưa deploy)
- `review_reports.query_held()` thêm nguồn `v2_job` → V2 held xuất hiện trong
  `/errors`, `/errors csv`, `/errors files` với `repair_caption=FIX_V2JOB:<name>`.
- `auto_alerts.next_alert()` thêm vòng lặp `upload_jobs state='held'` với key
  `v2job:<normalized_name>:<hash>` → báo Control Bot, nhấn mạnh **KHÔNG tự retry**.
- 6 test simulation mới chứng minh: V2 held xuất hiện trong report/CSV/alert,
  ambiguous không tự gửi lại, fault lock sống qua restart, ACK mất vẫn held,
  crash → held.

## 4. Reconciliation — vấn đề lớn nhất

`publish_guard.reconcile_flood_held()` dùng `scan_limit=25000`. Lịch sử kênh đích hiện
**>134,000 message**, nên mỗi chu kỳ 30 phút đều raise:

```
RuntimeError: Destination history exceeds safe scan limit of 25000; no recovery performed
```

Hệ quả:
- Các artifact `held` do FloodWait **không bao giờ được đối soát tự động**.
- Job `tech.035400805.edz` (FloodWait 1634s) vẫn held dù đã hết cooldown.
- Đây là **fail-closed đúng** (không requeue khi chưa đủ bằng chứng) nhưng là điểm nghẽn
  chức năng, không phải lỗi mất dữ liệu.

Đề xuất: port cơ chế `dest_inventory` + checkpoint (`dest_scan_resume` / watermark)
từ `telegram-downloader-rs` sang Python; chia nhỏ phân trang `offset_id`, lưu checkpoint
trong `settings`, và chỉ chuyển held→ready/posted sau khi quét **toàn bộ** lịch sử.

**Không** chuyển held→queued/ready khi chưa có bằng chứng (giữ nguyên nguyên tắc hiện tại).

## 5. Kiểm thử trên clone

Môi trường: SQLite file-backed riêng + Telegram client giả lập, không đụng production.

| Kịch bản | Kết quả |
|---|---|
| V2 held hiển thị trong `/errors` + CSV | ✅ (sau fix staging) |
| V2 held phát alert Control Bot | ✅ (sau fix staging) |
| Ambiguous → held, không tự gửi lại qua nhiều tick | ✅ |
| SQLite fault → khóa publisher, sống qua restart | ✅ |
| ACK mất sau Telegram nhận → vẫn held, không enqueue lại tên | ✅ |
| Crash lúc posting → held, `target_message=NULL` | ✅ |
| Same-name/diff-SHA, rename artifact, media protected, CRC demux | ✅ (các suite trước) |
| **Full suite** | **168/168 PASS**, compile OK |

## 6. Đề xuất theo mức độ

### HIGH
1. **Reconciliation 25k limit** — nâng lên phân trang + checkpoint như Rust reference.
   Hiện làm nghẽn toàn bộ luồng phục hồi FloodWait.
2. **V2 held vô hình trong `/errors` và alert** — đã sửa ở staging commit mới; cần deploy
   (có restart) sau phê duyệt.
3. **Watchdog timer inactive** — bật timer hoặc dùng systemd unit để có cảnh báo khi
   service down; hiện chỉ có alert in-process.

### MEDIUM
4. `auto_issues` thiếu vòng đời resolved → thêm cột `resolved_at` (additive) và thông báo
   phục hồi.
5. Phân loại rõ `known / new / needs-admin` trong `/errors` (hiện chỉ có category).
6. Cân nhắc báo FloodWait held (hiện bị `flood_error()` lọc khỏi alert artifact) để admin
   biết job cần đối soát.

### LOW
7. Dọn dẹp log reconciliation lặp 30 phút/lần (giảm noise journal).
8. Rà soát 306 artifact held lịch sử để phân loại lại sau khi có reconciliation mới.

## 8. Đợt hoàn thiện tiếp theo (staging, commit mới)

### 8.1 Lỗi đã sửa

| Vấn đề | Cách sửa |
|---|---|
| V2 held vô hình trong `/errors` | `query_held()` lấy thêm `upload_jobs`; summary có riêng **V2 Job Held**; `/errors <SHA256>` tra được cả V2 (ưu tiên V2 nếu trùng SHA) |
| CSV thiếu/không rõ nguồn V2 | CSV có cột `disposition` (`NEEDS_ADMIN` / `AWAITING_RECONCILIATION`), dedup theo `(type, key)`, **không còn `FIX_V2JOB`** (thay bằng `(chưa có lệnh sửa tự động)`) |
| Thông báo không phân biệt | Alert V2 ghi rõ "CẦN QUẢN TRỊ VIÊN" hoặc "ĐANG CHỜ ĐỐI SOÁT"; `/errors` chi tiết có dòng `Trạng thái:` tương ứng |
| Reconciliation kẹt ở 25k | Thay bằng `reconcile_held()`: phân trang `page_size`, cursor + found + watermark lưu trong `settings`, tiếp tục được sau gián đoạn; hỗ trợ lịch sử **150.000+** message |
| Chỉ đối soát V1 | Đối soát cả `artifacts` V1 và `upload_jobs` V2, khớp **tên chuẩn hóa + SHA256 + size + Message ID** |
| Suy luận "chưa đăng" từ quét chưa xong | **Bỏ hoàn toàn** hành động chuyển held→ready; chỉ đánh dấu posted khi có bằng chứng Telegram. Không retry ambiguous/protected, không đụng job không liên quan |
| FloodWait/mất mạng giữa trang | Lưu checkpoint rồi re-raise; bot áp cooldown; lần sau resume từ cursor. Không áp dụng kết quả nếu chưa quét xong |

`reconcile_flood_held()` được giữ làm wrapper tương thích; bot giờ kiểm tra
`has_held_reconcilable()` (cả V1 + V2) trước khi chạy.

### 8.2 Watchdog — nguyên nhân timer inactive

- Unit/timer tồn tại đúng: `OnCalendar=*-*-* *:0/2:00`, `Persistent=true`,
  `ExecStart=.../scripts/edz-server-watchdog-v16.py`, `ReadWritePaths=.../data`.
- `systemctl is-enabled edz-server-watchdog.timer` → **disabled**; timer chưa bao giờ
  được `enable --now`. Bot token/ERROR_CHAT đã cấu hình. Có timer Rust riêng đang chạy
  (`oumijia-rs-watchdog.timer`) nhưng không phủ service Python.
- **Không bật timer production** khi chưa phê duyệt. Đã test state machine bằng mock:
  phát hiện issue mới, thông báo phục hồi, phát hiện gap giám sát và restart count.

### 8.3 Kiểm thử

**177 PASS / 0 FAIL** trên clone (full suite), compile OK. Bổ sung:

- Lịch sử giả lập **150.000 message**, phân trang 1.000 → checkpoint → resume → posted đúng
  message #137500 cho cả artifact V1 và job V2, `left_held=0`.
- Mất mạng giữa trang → checkpoint được lưu, **không đổi trạng thái**, resume thành công.
- ACK thất lạc (V2 ambiguous) → đối soát thành `posted` + event `reconciled_ack`,
  **không hề gọi publish**.
- Cùng tên/SHA khác, sai size → **không khớp**, job vẫn `held`.
- Job không liên quan (PROTECTED) giữ nguyên.
- `/errors`: V2 held trong summary/CSV, tra cứu theo SHA, không có `FIX_V2JOB`.
- Watchdog mock: issue/recovery/gap/restart.

### 8.4 Còn tồn tại / chưa deploy

- **Production vẫn chạy code cũ** (`e4f0c52` cho Control Bot); đợt sửa này **chưa deploy**,
  chưa restart, chưa đổi DB/pacing/concurrency, không đăng file thử.
- Watchdog timer vẫn inactive (chờ phê duyệt bật).
- `auto_issues` vẫn không có cờ resolved (Medium).
- 306 artifact held lịch sử cần rà soát thủ công sau khi reconciliation mới chạy.

### 8.5 Hướng dẫn triển khai an toàn (chờ phê duyệt)

1. Backup SQLite (Backup API) + source + `.env` + systemd unit.
2. Deploy 4 file: `publish_guard.py`, `bot.py`, `review_reports.py`, `auto_alerts.py`
   (sha256 phải khớp commit mới). Không đụng `upload_engine.py`, `config.py`, `.env`.
3. Chạy `py_compile` + `pytest -q tests/` trên production source (hoặc clone cùng commit).
4. Chọn cửa sổ FloodWait cooldown còn hiệu quả / 0 job `posting`, **xin phép restart 1 lần**.
5. Sau restart: gửi `/errors` (phải có `V2 Job Held`), `/status`; kiểm tra log không có
   `Destination history exceeds...` nữa.
6. Cho reconciliation chạy có giám sát; nếu FloodWait → cooldown được tôn trọng, cursor
   được lưu. Không tự giải phóng held; chỉ admin đối soát các job còn lại.
7. Riêng watchdog: `systemctl enable --now edz-server-watchdog.timer` **chỉ khi có phê duyệt riêng**.

## 7/9. Kết luận

**PARTIAL — Production chưa được nâng cấp.**

Code staging đã chứng minh được quá trình đối soát V2 qua kiểm thử (177/177 PASS,
150k message, resume, lost ACK), nên **không còn lỗi FAIL ở mức code**. Tuy nhiên Production
vẫn đang chạy bản cũ: reconciliation 25k, V2 held chưa vào `/errors`, watchdog timer inactive.
Chỉ chuyển sang **PASS** sau khi deploy commit mới, verified `/errors` + reconciliation thực tế,
và xử lý watchdog theo phê duyệt. Không có bằng chứng mất dữ liệu hay đăng trùng.
