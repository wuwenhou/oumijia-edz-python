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

## 5b. PRODUCTION CANARY — KẾT QUẢ FAIL (an toàn, 2026-11-17)

**Phạm vi được phê duyệt:** 1 EDZ thật trên kênh đích `-1004349174061` sau khi
preflight PASS. Không mở backlog, không tăng concurrency.

### Preflight (đều PASS trước khi deploy)
- 5 điều kiện bắt buộc → 135/135 test PASS (sau đó 136/136 khi thêm fix media).
- Candidate: `TECH.1272001H9.edz` · SHA `e1b6e53be178f3113fdae5af1544014a4eec0d9f62c62f6446861919f3e7d3e5`
  · 31,308 bytes · state `ready` · **không** có trong `edz_name_catalog`, không posted,
  không có trên kênh đích (Telegram search = 0) · file trên disk đúng SHA/size.
- Backup: `data/backups/20261117-upload-v2-canary/` — SQLite **Backup API**,
  `quick_check=ok`, posted 132947 / held 306 / ready 6535, max msg 132952;
  backup source `.env` + systemd unit; sha256 ghi trong `SHA256SUMS`.
- Chỉ 1 Publisher (`python -m edzbot run` PID 3927992), Rust inactive.
- Deploy 6 file, sha256 local == production. Restart **1 lần**: PID 4047067, NRestarts=0.

### FloodWait — đã chờ đúng (tuân thủ)
Legacy publisher set cooldown đến **02:36:15 UTC** (FloodWait 01:41:18). Không xóa
khóa này; đợi đúng ~45 phút mới cho canary chạy.

### Kết quả thử — FAIL, KHÔNG có bài đăng nào được tạo
| Mục | Giá trị |
|---|---|
| Tên EDZ | TECH.1272001H9.edz |
| SHA256 / size | e1b6e53b…3e5 / 31,308 bytes |
| Job | `state=held`, `attempts=1`, `target_message=NULL` |
| Lỗi | `Ambiguous posting outcome: You can't forward messages from a protected chat (caused by SendMediaRequest)` |
| Bài trùng trên đích | **0** (search = []) |
| latest dest message id | **132952** (không đổi) |
| posted/held/ready trước | 132947 / 306 / 6535 |
| posted/held/ready sau | **132947 / 306 / 6535** (không đổi) |
| Duplicate post | 0 |
| FloodWait trong lượt thử | 0 (đã đợi cooldown cũ) |
| Auto-retry | **0** — held, không gửi lại |
| Control Bot | active, trả lời /status bình thường |
| `/v2stats` | **không trả lời** (lỗi riêng, xem bên dưới) |

### Nguyên nhân (đã xác nhận read-only)
`artifacts.origin='generated'` nhưng V2 vẫn cố reuse media từ `source_message=105`
(legacy chỉ reuse khi `origin='direct'`). Hơn nữa message có `noforwards=False`
nhưng `chat.noforwards=True` (kênh nguồn được bảo vệ) → Telegram từ chối.
Telegram **từ chối trước khi tạo bài** (max msg không đổi), nhưng engine phân loại
mọi exception không-FloodWait là `ambiguous` → `held`. Đúng chính sách "không tự
gửi lại khi kết quả không rõ".

### Đã xử lý trong staging (chưa deploy lại)
- Chỉ reuse media khi `origin='direct'` **và** `not message.noforwards`
  **và** `not message.chat.noforwards`; nếu không thì upload byte từ file local.
- Thêm cột `origin` vào `upload_jobs` (additive).
- Sửa `/v2stats`: xử lý trước `parse_command` (verb này không nằm trong parser).
- Test mới cho 2 trường hợp trên → **136/136 PASS**.

### Trạng thái production hiện tại
- `.env`: `UPLOAD_ENGINE_V2=false` (ghi nhận để lần start sau; **không restart lần 2**).
- Tiến trình đang chạy vẫn có V2 active nhưng **vô hiệu**: canary gate chỉ cho phép
  đúng 1 tên, job đó đã `held` → `claim_next()` không trả job nào. Không có bài nào
  sẽ được gửi thêm.
- Không restore DB cũ (không có bài đã ACK nào bị mất). Dữ liệu canary giữ nguyên để audit.
- **Cần phê duyệt mới** để deploy bản fix và thử lại với candidate khác.

## 5c. PRODUCTION CANARY 2 — PASS (2026-11-17)

Commit đã kiểm thử và deploy: **`cfa21580a3b56ebefd2f16dec070fdb589a72ea4`**.

### Preflight (đều PASS)
- Migration: `upload_jobs` production thiếu `origin`; clone DB thật → `ALTER TABLE
  ADD COLUMN origin` thêm đúng 1 cột, giữ nguyên job `held` của canary 1, chạy
  lần 2 không đổi gì. Không DROP/CREATE. PRAGMA trước/sau đã ghi nhận.
- Fail-closed guard: `PUBLISHER_MODE` mặc định `paused` → **cả V1 lẫn V2 đều không
  gửi**; `canary` bắt buộc V2=true + tên canary + concurrency 1/1; archive stream tắt.
- Test media matrix 6/6; restart guard: chỉ đúng job canary được enqueue/claim
  (4 artifact ready khác không bị chạm); `/v2stats` unit test PASS.
- **145/145 test PASS** trên clone.
- Backup mới qua Backup API: `data/backups/20261117-upload-v2-canary2/`
  sha `16e8fddf…`, quick_check ok.
- Candidate: `TECH.1314003.edz` · `cdcf2227…a036b` · 31,392 B · `origin=generated`
  · đúng SHA/size trên disk · 0 trong catalog, 0 posted, 0 trên Telegram đích
  · đúng 1 artifact `ready` với tên này (startup guard PASS).

### Kết quả
| Mục | Trước | Sau |
|---|---|---|
| posted | 132947 | **132948** |
| held | 306 | 306 (job canary 1 vẫn `held`) |
| ready | 6535 | **6534** |
| max target msg | 132952 | **132953** |
| Telegram msg ID | — | **132953** |
| caption xác minh | — | `File: TECH.1314003.edz`, `SHA-256: cdcf2227…a036b` ✓ |
| ACK SQLite | — | `upload_jobs(posted, target_message=132953, origin=generated)`, `artifacts(posted, normalized_name, msg=132953)`, event `ack` ✓ |
| FloodWait / retry / duplicate | — | **0 / 0 / 0** |
| Thời gian | — | restart 02:47:03 → posted 02:47:07 (~4s, log xác nhận) |
| Tốc độ | — | không báo MiB/s (file 31 KB, upload 1 chunk — không đại diện) |

- Service: PID **4049070**, NRestarts=0, active/running. Log: `CANARY GUARD PASS`,
  `Upload Engine V2 active (concurrency=1/1)`, `V2 Posted EDZ ... #132953`.
- `/v2stats` trả lời trên Control Bot (msg #3400/#3401); unit test cũng PASS.
- Hàng đợi sau canary: ONLY 2 job — canary1 `held`, canary2 `posted`. **Không mở backlog**.
- Gate đã chuyển `PUBLISHER_MODE=paused`, `UPLOAD_ENGINE_V2=false`, xóa tên canary
  trong `.env` ngay sau lượt thử (không restart lần 2). Restart sau đó → V1 lẫn V2
  đều không gửi.

### Ghi chú còn tồn
- `edz_name_catalog` **không có row** cho tên canary (ACK chỉ UPDATE nếu row đã tồn
  tại). Dedup vẫn đảm bảo qua `upload_name_owners` + `artifacts.normalized_name=posted`.
- Caption brand hiển thị `EPLAN EDZ | EPLAN EDZ` (artifact `origin=generated` không
  có brand) — cosmetic, không ảnh hưởng định danh SHA/EDZID.

## 5d. FULL ROLLOUT — ĐÃ KÍCH HOẠT (2026-11-17 03:25 UTC)

Commit deploy: **`124843c8b81fd37b5720913b4cc43a051a05ad6c`** (147/147 test PASS).

### Thay đổi code cho full rollout
- `PUBLISHER_MODE=normal` + `UPLOAD_ENGINE_V2=true` được phép; `normal` + V2=false
  bị **từ chối** (không bao giờ hồi sinh V1).
- `ack()` giờ **INSERT/UPDATE `edz_name_catalog`** trong cùng transaction → mọi
  tên đã đăng đều có row `state='accepted'` (117.123 → 117.293).
- Fault lock SQLite bền vững: lỗi sqlite3 → ghi `upload_engine_fault` → publisher
  khóa cả sau restart cho tới khi operator clear. Test fail-closed PASS.

### Triển khai
- Backup: `data/backups/20261117-v2-full-rollout/` (Backup API, quick_check ok,
  sha `84e9d21c…`), kèm source/`.env`/systemd unit.
- Config: `PUBLISHER_MODE=normal`, V2=true, 1/1, canary trống, archive stream
  false, source scan false, `SEND_DELAY_SECONDS=3`.
- Restart **1 lần**: PID **4050186**, NRestarts=0. Log:
  `Publisher started (mode=normal)`, `Upload Engine V2 active (concurrency=1/1, canary=OFF)`.
- Chỉ **1 tiến trình** `python -m edzbot run`; Rust inactive.

### Batch 10 → 100 → tiếp tục (tự động, không dừng xin phép)
| Mốc | posted (artifacts) | ready | jobs posted | catalog | Fault | Duplicate |
|---|---:|---:|---:|---:|---|---|
| Trước | 132948 | 6534 | 1 | 117123 | — | — |
| Sau 10 | 132959 | 6523 | 12 | — | None | 0 |
| Sau 100 | 133055 | 6427 | 108 | 117230 | None | 0 |
| 03:31 UTC | **133118** | **6364** | **171** | **117293** | None | 0 |

- Telegram ACK: mọi job posted đều có `target_message`; max target **133123**.
- Duplicate spot-check trên kênh đích: mỗi tên **đúng 1** message.
- FloodWait **0**, retry **0**, held job vẫn **1** (canary 1, không tự retry).
- `/v2stats` (msg #3421): `Đã đăng: 163 (5.47 MiB), 31.83 file/phút`, ACK 0.4s.
- Report đã gửi qua Control Bot (msg #3418).
- Đĩa trống **143 GiB**.

### Lỗi không chặn (đã ghi nhận)
Reconciliation worker (legacy) vấp giới hạn `scan_limit=25000` vì lịch sử đích
>133k message; nó **fail-closed**, không requeue held nào — đúng thiết kế. Cần
tăng scan_limit hoặc bỏ worker này ở bản sau; không ảnh hưởng V2.

Hệ thống tiếp tục chạy 24/7, tự tiêu thụ backlog còn lại với concurrency=1.

## 5e. TRUNG TÂM ĐIỀU KHIỂN (2026-11-17 04:00 UTC)

Commit: `398047c` (sau `76d3b71`) · **150/150 test PASS**.

- `/help` thay bằng đúng menu "🤖 OUMIJIA — TRUNG TÂM ĐIỀU KHIỂN" theo yêu cầu.
- `/status` bổ sung khối `🚀 Upload V2: BẬT (concurrency 1/1)`, đã đăng/MiB·s/
  file·phút, ACK TB, FloodWait, duplicate, hàng đợi, cờ FAULTED.
- Sửa 2 lỗi phát hiện khi verify live: duplicate bị đếm lại mỗi tick (→ đếm 1 lần
  cho mỗi tên) và thiếu xuống dòng trước dòng cooldown.
- Deploy 2 file, restart có kiểm soát: PID **4051500**, NRestarts=0, V2 tiếp tục
  đăng ngay (#134069). Backup `data/backups/20261117-control-center-menu/`.
- Xác nhận trực tiếp qua Control Bot: `/help` msg #3448, `/status` msg #3447 —
  `duplicate: 0`, xuống dòng đúng, V2 BẬT 1/1.

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
