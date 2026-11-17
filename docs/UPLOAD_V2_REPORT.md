# OUMIJIA Python — Upload Engine V2 (staging evidence report)

**Trạng thái:** staging only. Production (`edz-telegram.service`, Python v16.3.2)
**không bị thay đổi**. V2 nằm sau cờ `UPLOAD_ENGINE_V2=false` (mặc định), chưa có
cutover, chưa đụng dữ liệu lịch sử.

Repo: `https://github.com/wuwenhou/oumijia-edz-python` · nhánh
`staging/upload-engine-v2`.

---

## 1. Giai đoạn 1 — đối chiếu hai repo

| Kỹ thuật từ `telegram-downloader-rs` | Quyết định cho Python V2 |
|---|---|
| `7z l -slt` listing (Path/Size/CRC) + `7z x -so` decode **một lần**, demux theo size | ✅ Port sang `edzbot/stream_archive.py` |
| CRC-32 guard **trước khi gửi 1 byte** + SHA-256 | ✅ Port, CRC chỉ check khi có metadata tin cậy |
| `upload_jobs` checkpoint, ghi trước khi sang file kế | ✅ Bảng `upload_jobs` + ACK transaction |
| FloodWait = tín hiệu, sleep đúng giây, budget riêng | ✅ Giữ nguyên qua `publish_guard` + `_on_flood` |
| Bounded parallel upload (1–2) | ✅ `AdaptiveGovernor`, hard cap 2 |
| `dest_inventory` / scan checkpoint | ⚠️ Không copy nguyên; dùng lại `edz_name_catalog` + guard hiện có để **không thêm DB thứ hai** |
| `winsqlite3.dll`, named pipe, HKCU autostart, `7z.exe`, `%APPDATA%` session | ❌ Không port (Windows-only, sản phẩm chạy Ubuntu) |
| Publisher riêng (`tgdlrs upload`) | ❌ Không dùng — V2 nằm **trong** tiến trình Python hiện tại, chỉ 1 Publisher |

Kết luận tốc độ (đã đo thực tế trước khi thiết kế): Python hiện đạt **~20–27
bài/phút** (9.327 bài / 7h43, 2.842 bài / 1h43), ngang/nhanh hơn mức README
của tgdlrs (9–17 file/phút). Nút thắt là **uplink/Telegram ACK**, không phải code.
Vì vậy mục tiêu V2 là **ổn định + streaming + chống trùng + thống kê**, không phải
hứa tăng tốc thần kỳ.

---

## 2. Kiến trúc V2

### File mới / thay đổi
- `edzbot/upload_engine.py` (mới) — engine, governor, stats, ACK transaction, recovery, archive driver.
- `edzbot/stream_archive.py` (mới) — listing + single-pass demux, CRC/SHA, temp file bounded.
- `edzbot/db.py` — **chỉ thêm 3 bảng** (`upload_name_owners`, `upload_jobs`, `upload_events`); không ALTER/DROP bảng cũ.
- `edzbot/config.py` — 5 cờ cấu hình mới, mặc định an toàn (V2 tắt).
- `edzbot/bot.py` — V2 được gọi từ **chính `publish_worker_v163`** (một Publisher); cổng điều khiển `/v2stats`.
- `edzbot/name_guard.py` — chuẩn hóa thống nhất **NFC + casefold**.
- `tools/bench_upload_v2.py`, `tools/rehearse_v2.py` — bằng chứng hiệu năng/phục hồi.

### Schema bổ sung (additive)
```
upload_name_owners(normalized_name PK, sha256, file_name, size_bytes, source_message, created_at)
upload_jobs(normalized_name PK, sha256, file_name, path, size_bytes, source_message,
            state, attempts, target_message, error, created_at, updated_at)
upload_events(id, at, normalized_name, kind, detail)
```

### Quy tắc chống trùng (đúng spec V5)
- `claim_name()` dùng `INSERT OR IGNORE` vào `upload_name_owners` → **atomic**
  trước khi upload. Cùng tên chuẩn hóa → duplicate, không bao giờ đăng lại bất kể SHA.
- Khác tên → mỗi tên một row `upload_jobs`, **kể cả SHA-256 giống nhau**. Đây là cách
  giải quyết giới hạn `artifacts.sha256 PRIMARY KEY` **mà không sửa bảng lịch sử**.
- ACK trong **một transaction**: `upload_jobs.posted + target_message`,
  `artifacts.posted + normalized_name` (nếu cột tồn tại), `edz_name_catalog.accepted`,
  `upload_events`.

### An toàn ACK / recovery
- Timeout/generic exception → `held` ("Ambiguous posting outcome"), **không auto retry**.
- `posting` tồn tại sau crash → `recover_posting()` chuyển `held` khi restart.
- Pre-send failure (file mất/sai size/sai SHA/quá limit) → `held`, retry **có giới hạn**
  do thiết kế (không tự gửi lại khi đã qua lúc gọi `send_file`).
- FloodWait → sleep đúng giây, set cooldown dùng chung với Control Bot, hạ concurrency về 1.

### 7z streaming
- Đọc listing trước, lọc `.edz`, bỏ nested EDZ, lấy CRC nếu có.
- Decode **một lần**, demux theo size; Solid archive = stream tuần tự — **không tuyên bố random access**.
- File đi qua `NamedTemporaryFile` trong `UPLOAD_TEMP_DIR` (RAM bị chặn ở chunk 256 KiB);
  **1 file tạm tồn tại tại một thời điểm** (enqueue → publish → cleanup).
- Không gọi `edzsplit` cho EDZ lấy từ 7z; CRC sai → abort **trước** khi gửi byte nào.

---

## 3. Giai đoạn 8 — kiểm thử (trên SQLite clone Ubuntu)

```
128 passed in 3.25s     (109 baseline + 19 test V2)
py_compile: OK
```

19 test mới phủ: normalize NFC+casefold; cùng tên/khác SHA; khác tên/cùng SHA;
ACK một transaction; ACK rollback khi SQLite lỗi; ACK trên DB legacy không có cột
`normalized_name`; ambiguous → held; restart → held; FloodWait hạ concurrency +
set cooldown + sleep đúng; governor phục hồi sau streak; stats MiB/s & file/phút;
giới hạn concurrency (1 và 2); không duplicate sau restart; reuse media nguồn;
listing CRC/nested; demux đúng; skip tên đã nhận vẫn căn chỉnh stream; CRC mismatch
abort; stream cụt abort.

---

## 4. Giai đoạn 7 — hiệu năng (mock ACK latency 0.08s, 60 file × 1 MiB, SHA thật)

| Cấu hình | Thời gian | MiB/s | file/phút |
|---|---:|---:|---:|
| Publisher cũ (tuần tự, c=1) | 5.27s | 11.38 | 682.9 |
| V2 concurrency=1 | 5.73s | 10.47 | 628.1 |
| **V2 concurrency=2** | **3.25s** | **18.44** | **1106.1** |

RAM đỉnh: **56.2 MiB**. (`.venv/bin/python tools/bench_upload_v2.py`)

**Giải thích trung thực:** concurrency=2 chỉ nhanh hơn khi nút thắt là **độ trễ ACK**
(mock ở đây). V2 c=1 chậm hơn cũ ~0.5s/60 file do chi phí hàng đợi/transaction —
đây là cái giá của độ bền. Trên Telegram thật (uplink ~1.7–2 MB/s + FloodWait), c=2
**không nhân đôi băng thông** và dễ dội rate-limit. Vì vậy:
- **Mặc định `UPLOAD_CONCURRENCY=1`**, tối đa 2.
- Governor tự nâng lên 2 sau 25 lần ACK liên tiếp ổn định, tụt về 1 ngay khi FloodWait.
- Con số thật phải đo lại bằng kênh test có phê duyệt trước khi đổi mặc định.

## 5. Clone rehearsal (file-backed SQLite, giả crash)

```
recovered_posting_to_held: 2      held_after_restart: 2
held_after_manual_reconcile: 1    posted: 1
same_name_re_enqueue_blocked: true
ambiguous_jobs_auto_resent: []    no_auto_resend: true
```

→ Không có job mơ hồ nào tự gửi lại; tên đã posted không thể enqueue lại.

---

## 6. Giới hạn đã biết / rủi ro phải biết

1. **`artifacts.sha256 PRIMARY KEY` vẫn còn** — V2 tách danh tính tên sang bảng phụ,
   nên xử lý đúng "khác tên cùng SHA" ở luồng đăng. Nhưng nếu một ngày cần lưu 2 bản
   đó đồng thời trong bảng `artifacts`, **phải migrate có kiểm soát** (không làm ở đây).
2. **Không hứa resume upload mức byte.** Telethon/API không có cơ chế được kiểm chứng
   ở đây; V2 chỉ resume ở mức **job** (chưa posted thì gửi lại từ đầu, đã mơ hồ thì held).
3. **Solid 7z đọc tuần tự** — decode lại từ đầu block nếu bỏ qua phần giữa; ta decode
   một lần nên O(tổng bytes), không phải O(n²).
4. Casefold khác `lower()` với vài ký tự Unicode hiếm (ß). Tên EDZ thực tế đều ASCII,
   nên không ảnh hưởng; catalog cũ được build với `lower()` — đã test tương thích.
5. Benchmark là **mock**, không phải Telegram thật — xem mục 4.

---

## 7. Backup / Deploy / Rollback runbook (chỉ sau khi được phê duyệt)

### Backup (bắt buộc trước cutover)
```bash
B=/home/cardanobp/edz-telegram-production/data/backups/$(date +%Y%m%d-%H%M%S)-upload-v2
mkdir -p "$B"
cp -a /home/cardanobp/edz-telegram-production/data/state.sqlite3 "$B/state.sqlite3"
cp -a /home/cardanobp/edz-telegram-production/.env "$B/env.bak"
cp -a /home/cardanobp/edz-telegram-production/edzbot "$B/edzbot-src"
sha256sum "$B/state.sqlite3" | tee "$B/SHA256SUMS"
# Verify backup có đủ bảng + quick_check trước khi làm gì tiếp theo
python3 -c "import sqlite3; d=sqlite3.connect('$B/state.sqlite3?mode=ro',uri=True); print(d.execute('PRAGMA quick_check').fetchone()[0], d.execute('SELECT COUNT(*) FROM artifacts').fetchone()[0])"
```

### Deploy (1 restart có kiểm soát)
1. Xác nhận service đang chạy, ghi `MainPID`/`NRestarts`.
2. Deploy source từ nhánh `staging/upload-engine-v2` (đã test) vào production.
3. Thêm vào `.env`: `UPLOAD_ENGINE_V2=true`, `UPLOAD_CONCURRENCY=1`, `UPLOAD_MAX_CONCURRENCY=2`.
4. `python -m py_compile edzbot/*.py` + `.venv/bin/python -m pytest -q tests/`.
5. `systemctl restart edz-telegram.service`; kiểm tra `ActiveState=active`, log có
   `Upload Engine V2 active (concurrency=1/2)`, `Authenticated ...`.
6. Theo dõi 30 phút: `/status`, `/v2stats`, số `held`, FloodWait, `journalctl -u edz-telegram`.
7. **Không** bật Legacy Source, **không** tự retry held, **không** gửi bài test lên kênh thật.

### Rollback (nếu V2 gây lỗi)
```bash
# Tắt cờ trước khi làm gì khác
sed -i 's/^UPLOAD_ENGINE_V2=true/UPLOAD_ENGINE_V2=false/' /home/cardanobp/edz-telegram-production/.env
# Khôi phục source cũ từ backup (hoặc git checkout 5f95e9b)
cp -a "$B/edzbot-src" /home/cardanobp/edz-telegram-production/edzbot
echo <password> | sudo -S systemctl restart edz-telegram.service
```
- Bảng V2 (`upload_*`) là additive: **không cần xóa** khi rollback. Để nguyên để audit.
- Nếu cần khôi phục DB: dừng service, copy backup vào `data/state.sqlite3`, xác minh
  `quick_check=ok`, rồi start lại. Lưu ý mọi bài đăng sau lúc backup sẽ bị mất khỏi DB
  → chỉ làm khi thực sự cần và đã đối chiếu Telegram đích.
- Không quay lại Rust lúc này; nếu có, nhớ cờ `telegram_posts_seeded` như đã ghi ở
  báo cáo trước (bảng đã bị drop).

### Tiêu chí phê duyệt cutover
- [ ] 128/128 test PASS trên clone (đã xong).
- [ ] Benchmark + rehearsal được review (đã xong).
- [ ] Backup mới + verify (làm ngay trước giờ cutover).
- [ ] Kênh test riêng để đo throughput thật (chưa có — **bắt buộc** trước khi đổi mặc định concurrency).
- [ ] Người phê duyệt xác nhận "chỉ 1 Publisher, không test post lên production".
