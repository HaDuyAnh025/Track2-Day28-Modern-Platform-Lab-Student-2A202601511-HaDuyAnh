# ANSWERS — Day 28 Track 2

Người thực hiện: Hà Duy Anh (2A202601511), làm cá nhân qua đủ 4 vai trò kỹ thuật
(Ingestion & Orchestration, Data & ML, Serving & Retrieval, Platform &
Observability) cộng vai Presenter.

## 1. Trạng thái 10 điểm kết nối (chạy thật, `--profile full`)

| IP | Trạng thái | Bằng chứng |
|---|---|---|
| IP01 Kafka | **ready** | `evidence/ip01-kafka-consume.json` — message thật trên `data.raw` có cả `traceparent` và `idempotency-key` |
| IP02 Airflow | **ready** | `evidence/ip02-airflow-run.json` — DAG `lab28_ingestion_pipeline` run `it-0055bd7f`, cả 4 task `success`, 4 asset event phát ra |
| IP03 Delta | **ready** | `evidence/ip03-delta-history.json` — `documents v6 (17 rows), feedback v9 (19 rows)` |
| IP04 Feast | **ready** | `evidence/ip04-feast-online.json` — entity thật, mọi feature `PRESENT`, `delta_version: 4`, `freshness_seconds: 60` |
| IP05 Qdrant | **ready** | `evidence/ip05-qdrant-search.json` — 17 điểm, ID xác định |
| IP06 MLflow | **ready** | `evidence/ip06-mlflow-release.json` — `lab28-rag-release` v3, alias `champion` |
| IP07 vLLM | not_ready (đúng dự kiến) | `evidence/ip07-vllm-identity.json` — `is_real_vllm: false`, không giả server; cần endpoint GPU thật |
| IP08 Gateway | **ready** | `evidence/ip08-gateway.json` — 30 request thật qua Envoy: 11 accepted / 19 rejected (429), có `x-request-id` |
| IP09 Prometheus/Grafana | **ready** | `evidence/ip09-prometheus-targets.json` + `evidence/ip09-grafana-dashboards.json` |
| IP10 Trace | **11/11 span bắt buộc** | `evidence/ip10-trace.json` (6 span: gateway→api→kafka→airflow→spark) + trace `/ask` thu riêng (5 span còn lại: feast/qdrant/mlflow/vllm) |

`lab28 integration` (điểm serving tự kiểm chứng được): **83/100**, 5/6 điểm
`ready` (chỉ IP07 `not_ready` vì chưa có vLLM thật — đúng thiết kế, không giả
lập). `uv run pytest integration-tests -m "not gpu and not langsmith" -q`:
**56 passed, 16 deselected** (toàn bộ gate GPU/LangSmith deselect đúng vì
không có endpoint credential). `/ready` (API thật): `degraded` — chỉ vLLM.

## 2. Trade-off kỹ thuật đáng chú ý

- **Khóa dedupe là `(occurred_at, event_id)`, không phải offset Kafka.** Offset
  chỉ có ý nghĩa trong một partition; hai bản ghi cùng `idempotency_key` có thể
  rơi vào partition khác nhau qua các lần gửi lại. So theo cặp giá trị nghiệp
  vụ giữ tính xác định bất kể Kafka phân phối lại thế nào, đổi lại phải tin
  tưởng đồng hồ producer thay vì thứ tự giao hàng. Được xác nhận trực tiếp bởi
  `IT-J2-idempotent-replay` (9/9 pass): gửi lại nguyên lô không tăng số dòng.
- **`readiness_status` tách `mandatory` khỏi `ready`.** Feast và vLLM có thể
  lỗi mà không kéo `/ready` xuống `not_ready` (`LAB28_VLLM_REQUIRE_REAL` mặc
  định `false` trong container, nhưng `true` khi chạy CLI trực tiếp ở host —
  hai môi trường cùng code nhưng khác cấu hình mặc định, dễ gây hiểu nhầm
  `not_ready` giả nếu không đọc kỹ biến môi trường trước khi so sánh với
  `/ready` thật của API). Trade-off: một pod có thể "ready" nhưng trả câu trả
  lời suy giảm chất lượng — chấp nhận được cho tính sẵn sàng của gateway,
  nhưng phải giám sát riêng `degraded_reasons`.
- **Envoy rate limit cục bộ (10 token/giây, không phân biệt client).** Đơn
  giản, không cần Redis, nhưng là per-instance chứ không phải per-tenant.
  Bằng chứng thật: `lab28 seed --via-gateway` (13 tài liệu + 12 feedback gửi
  gần như đồng thời) liên tục bị 429 ở phần feedback vì 13 request tài liệu
  đã ăn gần hết ngân sách token trước đó — lặp lại nhiều lần vẫn ra cùng kết
  quả vì mẫu hình luôn giống nhau, không phải may rủi. Đã thêm
  retry-with-backoff phía client (`src/lab28_platform/cli.py`, hàm `seed`,
  5 lần thử lại × 0.3s khi gặp 429) — đúng hành vi một client thật nên có với
  API có rate limit, không phải sửa để "qua bài". Test chính thức
  `IT-gateway-rate-limit` đo con số thật: 30 request → 11 accepted / 19
  rejected — xác nhận giới hạn hoạt động đúng dưới tải cao hơn cả seed script.
- **`/ask` không có chế độ trả lời khi vLLM chết** (503 `dependency_unavailable`,
  không trả câu trả lời giả). Trade-off: an toàn hơn (không bịa câu trả lời)
  nhưng nghĩa là "degraded" ở `/ready` không đồng nghĩa "vẫn phục vụ được" ở
  đường dẫn suy luận chính — khoảng cách này được nói rõ khi demo.
- **DAG dùng `schedule=None`, kích hoạt tường minh (asset-driven, không phải
  cron).** Trade-off: không có độ trễ "chờ lịch" khi demo/test (kích hoạt
  ngay lập tức qua API), nhưng đổi lại hệ thống không tự "drain" Kafka theo
  thời gian thực — dữ liệu ngồi trong `data.raw` cho đến khi có ai (test,
  script, hoặc một scheduler khác đặt trên DAG này) gọi trigger.

## 3. Khoảng trống production và bài học vận hành thật

- **Sự cố có thật trong lúc làm bài, đã khôi phục thành công:** máy chạy
  Windows 16 GB RAM, ban đầu có 2 project Docker khác (`p-084-*`) chiếm cổng
  6333/8000. Lần đầu bật `--profile full` (thêm Airflow + Spark Connect),
  Docker Desktop's backend crash hai lần liên tiếp, một lần với lỗi
  `Read-only file system` giữa lúc `pip install` — dấu hiệu VM/WSL2 phía sau
  Docker Desktop hỏng đĩa tạm dưới áp lực RAM, không đơn thuần "chờ lâu". Xử
  lý: `wsl --shutdown`, khởi động lại Docker Desktop sạch, giải phóng RAM
  (dừng 2 project không liên quan), rồi chạy lại — lần này `--profile full`
  lên đủ 14/14 container `healthy` và toàn bộ 56 integration test pass. Bằng
  chứng không mất dữ liệu: MLflow version tăng dần qua các lần container khởi
  động lại (volume giữ nguyên), Qdrant/Kafka không mất điểm/topic nào.
- **Production gap #1 — chưa có vLLM thật (IP07 GPU gate).** Đây là khoảng
  trống duy nhất còn lại trong 10 điểm kết nối. Cần endpoint GPU thật
  (Kaggle/cluster) để nối `LAB28_VLLM_BASE_URL`; hệ thống đúng đắn báo
  `not_ready`/`degraded` thay vì giả lập. Làm theo `KAGGLE_GPU_EXTENSION.md`
  khi có endpoint.
- **Production gap #2 — cổng cứng trong `compose.yaml`/`ports.template`.**
  Qdrant (6333) và API (8000) không có sẵn override trong `ports.template`
  gốc dù compose hỗ trợ qua biến môi trường — một máy chạy song song nhiều
  bài lab dễ bị "port is already allocated" mà không rõ cách sửa nhanh nếu
  không đọc kỹ compose.yaml.
- **Production gap #3 — rate limiter không phân biệt nguồn.** Phù hợp cho demo
  một máy, nhưng production cần rate limit theo client (API key/IP) chứ không
  phải một token bucket toàn cục 10/giây dùng chung cho mọi người gọi — một
  client "nặng tay" (như script seed ban đầu) có thể vô tình khóa luôn các
  client khác trong cùng cửa sổ 1 giây.
- **Production gap #4 — vận hành cần RAM đúng như khuyến nghị.** README ghi rõ
  12–16 GB cho hệ thống toàn bộ; con số này là *tối thiểu thực tế*, không
  phải ước lượng an toàn dư dả — trên máy 16 GB có sẵn ứng dụng khác (trình
  duyệt, IDE), việc chạy `--profile full` cần dọn RAM chủ động trước, không
  thể "chạy song song" mà không rủi ro crash VM nền của Docker Desktop.

## 4. Đóng góp (làm cá nhân)

Một người thực hiện toàn bộ 4 vai trò kỹ thuật theo `docs/team-role-cards.md`:

- **Ingestion & Orchestration (IP01–IP02):** cài `event_headers`
  (`src/lab28_platform/integration_tasks.py`); xác minh Kafka header thật và
  toàn bộ DAG `lab28_ingestion_pipeline` chạy `success` qua Airflow thật.
- **Data & ML (IP03–IP04–IP06):** cài `dedupe_latest`, xác minh qua
  `tests/test_delta_merge_idempotency.py` và `IT-J2-idempotent-replay`; chạy
  `lab28 release` lấy MLflow champion v3; xác minh Feast trả feature thật sau
  materialize (không còn `NOT_FOUND`).
- **Serving & Retrieval (IP05–IP07):** cài `feast_online_request`; index tài
  liệu vào Qdrant (17 điểm); xác minh vLLM báo đúng "không phải server thật"
  thay vì giả — IP07 là gap duy nhất còn lại, phụ thuộc endpoint GPU.
- **Platform & Observability (IP08–IP10):** cài `readiness_status`; đo Envoy
  rate limit thật dưới tải (30 request, 11/19); xác nhận toàn bộ Prometheus
  target `up` và dashboard Grafana; thu đủ 11/11 span bắt buộc qua Jaeger; sửa
  `lab28 seed` để retry khi bị 429 thay vì báo lỗi giả.
- **Presenter:** biên soạn tài liệu này, thứ tự demo (mục 5), và mục Q&A
  (mục 6); ghi lại sự cố Docker Desktop và cách khôi phục làm bằng chứng cho
  phần "một sự cố → khôi phục → không mất dữ liệu" của demo.

## 5. Thứ tự demo đề xuất

1. Kiến trúc + 10 điểm kết nối (bảng ở mục 1), nêu rõ IP07 là gap duy nhất và lý do.
2. Happy path: `lab28 seed --via-gateway` → `evidence/ip01-kafka-consume.json`
   (traceparent + idempotency-key) → Airflow DAG chạy `success`
   (`evidence/ip02-airflow-run.json`) → Delta có version mới
   (`evidence/ip03-delta-history.json`) → Feast/Qdrant/MLflow cập nhật.
3. Idempotency: chạy `uv run pytest integration-tests/test_j2_idempotent_replay.py -q`,
   giải thích khóa `(occurred_at, event_id)` và kết quả gửi lại không tăng số dòng.
4. Sự cố thật: kể lại sự cố Docker Desktop crash khi bật `--profile full` lần
   đầu (mục 3) — dự đoán tín hiệu (RAM thấp) → quan sát (`Read-only file
   system`) → khôi phục (`wsl --shutdown`, dọn RAM, restart) → chứng minh
   không mất dữ liệu (MLflow/Qdrant/Kafka giữ nguyên qua các lần container
   khởi động lại) → chạy lại thành công đủ 14/14 container + 56/56 test.
5. Golden signals: mở `evidence/ip09-prometheus-targets.json` + Grafana
   dashboard "Lab 28 Platform Overview"; mở Jaeger với trace ID trong
   `evidence/ip10-trace.json` (span xuyên gateway→api→kafka→airflow→spark).
6. Rate limit / gateway: `evidence/ip08-gateway.json` (30 request, 11/19),
   giải thích vì sao seed ban đầu bị 429 và cách sửa (retry-with-backoff).
7. `ready`/`degraded`/`not_ready`: `curl http://localhost:8000/ready` → `degraded`,
   đối chiếu với `readiness_status` trong `integration_tasks.py`.
8. K8s/GitOps: `uv run python scripts/validate_manifests.py` (pass), giải
   thích các ràng buộc (non-root, không `:latest`, `targetRevision` pinned).
9. Q&A.

## 6. Chuẩn bị Q&A

**Vì sao IP07 vẫn `not_ready`?**
Chưa có endpoint GPU thật (Kaggle/cluster) để nối `LAB28_VLLM_BASE_URL`. Theo
đúng quy tắc của bài ("không làm giả vLLM"), hệ thống báo đúng trạng thái thay
vì dựng một server giả bắt chước OpenAI API — gate IP07 kiểm tra `/version`
thật của vLLM và metric `vllm:`, một server giả sẽ fail gate này ngay.

**Vì sao `/ready` báo `degraded` nhưng `/ask` vẫn trả lỗi 503?**
`/ready` coi vLLM là không bắt buộc (`mandatory=false` khi
`LAB28_VLLM_REQUIRE_REAL=false`, cấu hình mặc định trong container) nên pod
vẫn được đưa vào rotation của gateway — đúng cho các endpoint không cần LLM.
Nhưng `/ask` cần một câu trả lời thật từ LLM để không bịa nội dung, nên khi
vLLM không có, nó trả `dependency_unavailable` (503) chứ không đoán câu trả lời.

**Vì sao seed ban đầu có request bị từ chối (429)?**
Envoy giới hạn 10 token/giây dùng chung cho toàn bộ traffic qua gateway; script
seed gửi 13 tài liệu rồi ngay lập tức 12 feedback gần như đồng thời, ăn hết
ngân sách token trước khi đến phần feedback. Đây là bằng chứng IP08 hoạt động
đúng, không phải lỗi — đã sửa client để retry theo backoff (5 lần × 0.3s) thay
vì tăng giới hạn (tránh che giấu hành vi rate limit thật). Test chính thức đo
được con số tương tự dưới tải cao hơn: 30 request → 11/19.

**Sự cố Docker Desktop crash — bài học gì?**
RAM 16 GB là *ranh giới thực sự*, không phải margin an toàn, khi chạy đủ
Kafka+Airflow+Spark+MLflow+Qdrant+Prometheus+Grafana+Jaeger+Envoy cùng lúc với
ứng dụng khác (trình duyệt, IDE) đang mở. Bài học vận hành: trước khi chạy
`--profile full`, chủ động dọn RAM (đóng ứng dụng không cần, dừng project
Docker khác) thay vì thử rồi chờ; nếu backend Docker crash, `wsl --shutdown`
+ khởi động lại sạch khôi phục được service mà không mất dữ liệu volume.

**Nếu có thêm thời gian, sẽ làm gì tiếp?**
Nối vLLM thật theo `KAGGLE_GPU_EXTENSION.md` để IP07 chuyển từ `not_ready`
sang `ready`, đưa điểm tích hợp từ 83/100 lên tối đa; đồng thời chạy lại toàn
bộ 5 journey (J1–J5) một lần nữa sau khi có vLLM thật để bỏ được cờ GPU-gate
skip (hiện 3 test đang `skipped` vì gate này).
