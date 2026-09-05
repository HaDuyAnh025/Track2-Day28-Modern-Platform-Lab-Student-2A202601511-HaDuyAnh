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
| IP06 MLflow | **ready** | `evidence/ip06-mlflow-release.json` + `evidence/ip06-promotion-rollback.json` — promote v3→v6 rồi rollback v6→v5, serving đổi version ngay, không sửa code |
| IP07 vLLM | **ready** | `evidence/ip07-vllm-identity.json` — `is_real_vllm: true`, vLLM 0.26.0 thật trên Kaggle T4 (`Qwen/Qwen3-1.7B`), tunnel qua `cloudflared`; `/version`, `/v1/models`, `vllm:` metric đều xác nhận |
| IP08 Gateway | **ready** | `evidence/ip08-gateway.json` — 30 request thật qua Envoy: 11 accepted / 19 rejected (429), có `x-request-id` |
| IP09 Prometheus/Grafana | **ready** | `evidence/ip09-prometheus-targets.json` + `evidence/ip09-grafana-dashboards.json` |
| IP10 Trace | **11/11 span bắt buộc** | `evidence/ip10-trace.json` (6 span: gateway→api→kafka→airflow→spark) + trace `/ask` thu riêng (5 span còn lại: feast/qdrant/mlflow/vllm) |

`lab28 integration` (điểm serving tự kiểm chứng được): **6/6 điểm `ready`**
(IP01, IP03, IP04, IP05, IP06, IP07 — không còn điểm nào `not_ready`).
`uv run pytest integration-tests -m "not gpu and not langsmith" -q`:
**56 passed, 16 deselected**. Sau khi nối vLLM thật, chạy tiếp
`-m "gpu and not langsmith"`: **12 passed, 3 failed** — 3 lỗi này là giới hạn
hạ tầng có thật, không phải lỗi của 4 hàm TODO, xem Production gap #5–#7 ở
mục 3. `/ready` (API thật): **`ready`** — cả 5/5 thành phần (kafka, mlflow,
qdrant, vllm, feast) đều `ready`.

**Load profile** (`load-tests/run_profile.py --requests 200 --workers 8`,
`evidence/load-profile.json`): 200/200 request `/ready` qua gateway thành
công cả hai lần đo, nhưng độ trễ thay đổi rõ rệt theo cấu hình vLLM:

| Cấu hình vLLM | P50 | P95 | P99 |
|---|---:|---:|---:|
| Chưa nối (unreachable, fail nhanh) | 942 ms | 1474 ms | 2675 ms |
| Đã nối Kaggle T4 qua tunnel liên lục địa | 2569 ms | 4827 ms | 6928 ms |

**Bottleneck**: `/ready` gọi `probe_vllm` mỗi lần được hỏi (không cache), và
khi vLLM ở xa (Kaggle, qua `cloudflared`), riêng vòng round-trip xuyên lục địa
đã chiếm phần lớn độ trễ — đây là chi phí thật của việc dùng GPU mượn thay vì
GPU tại chỗ, không phải lỗi code. Gợi ý sửa cho production: cache kết quả
probe vLLM trong vài giây (TTL ngắn) thay vì gọi lại mỗi request `/ready`.

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

- **Sự cố chủ động tạo ra (failure injection) — dừng Qdrant, quan sát, khôi
  phục, chứng minh không mất dữ liệu.** Bằng chứng:
  `evidence/ip05-failure-injection-recovery.json`.
  - *Dự đoán tín hiệu*: Qdrant là probe bắt buộc (`mandatory=true` mặc định
    trong `probe_qdrant`, khác Feast/vLLM có `mandatory=False`/tuỳ cấu hình) —
    dừng nó phải làm `/ready` chuyển `not_ready` và gateway trả `503`.
  - *Nguyên nhân (tự tạo)*: `docker stop lab28-platform-qdrant-1`
    (08:03:42 UTC).
  - *Quan sát*: `/ready` → `not_ready`, component `qdrant`:
    `{"ready": false, "detail": "0 points; ResponseHandlingException: [Errno
    -2] Name or service not known"}`; `curl http://localhost:8080/ready` (qua
    Envoy) → `503`.
  - *Khôi phục*: `docker start lab28-platform-qdrant-1` (08:04:36 UTC),
    container chuyển `starting → healthy` sau ~4 giây.
  - *Không mất dữ liệu*: điểm dữ liệu Qdrant **giữ nguyên 18 điểm** trước và
    sau sự cố (`qdrant-data` volume không đổi khi container dừng) — outage chỉ
    là mất khả dụng tạm thời, không mất dữ liệu.
- **Sự cố thật #1 — Docker Desktop crash, đã khôi phục thành công:** máy chạy
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
- **Production gap #1 — đã đóng: vLLM thật qua Kaggle T4.** Nối theo
  `KAGGLE_GPU_EXTENSION.md`, gặp một sự cố thật khi khởi động: `vllm serve`
  crash ngay khi import với `OSError: Could not load this library:
  libtorchcodec_image.so` — bản `torchcodec` mà `pip install vllm==0.26.0` kéo
  về không tương thích với `torch`/`ffmpeg` có sẵn trên máy Kaggle, và đoạn
  import của vLLM chỉ bắt `ImportError` chứ không bắt `OSError` nên toàn bộ
  server sập thay vì bỏ qua tính năng video (không cần cho model text-only).
  Khắc phục: `pip uninstall -y torchcodec` để import thất bại đúng kiểu
  `ModuleNotFoundError` (được vLLM bỏ qua), khởi động lại — vLLM lên bình
  thường. Expose ra ngoài bằng `cloudflared tunnel --url http://127.0.0.1:8000`
  (quick tunnel, không cần tài khoản). Kết quả: `/version` báo `0.26.0`,
  `/v1/models` có đúng `Qwen/Qwen3-1.7B`, `/metrics` có 111 series `vllm:` —
  IP07 chuyển từ `not_ready` sang `ready`, `/ask` trả lời thật (query thật, độ
  trễ `llm_ms` đo được ~6.6–8s trên T4 free tier).
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
- **Production gap #5 — gateway không chủ động loại pod `not_ready` khỏi
  rotation.** `IT-J4` (`test_the_gateway_stops_routing_to_a_pod_that_is_not_ready`)
  thất bại thật: Envoy chỉ active health-check `/health` (liveness, luôn 200)
  cho cluster `api`, không có `outlier_detection` dựa trên response code của
  `/ready`. Khi Qdrant chết, API tự báo `/ready` → `not_ready` đúng, nhưng
  gateway vẫn định tuyến request tới đúng pod đó thay vì loại nó khỏi vòng
  luân chuyển — client vẫn nhận được breakdown JSON chi tiết (không phải lỗi
  "opaque" của Envoy), nhưng gateway không tự bảo vệ khỏi một pod đã biết là
  chưa sẵn sàng. Cần thêm `outlier_detection` (passive health check dựa trên
  `5xx`) vào `gateway/envoy.yaml`.
- **Production gap #6 — Prometheus scrape target cho vLLM giả định GPU local.**
  `test_the_inference_endpoint_is_scraped` thất bại vì job tĩnh
  `lab28-vllm-optional` trong cấu hình Prometheus trỏ cứng
  `host.docker.internal:8001` (đúng cho `compose.gpu.yaml` — GPU cắm trực
  tiếp), nhưng vLLM thật của mình chạy trên Kaggle qua tunnel, không phải cổng
  local đó — job báo `down` đúng như thực tế, nhưng cấu hình chưa hỗ trợ
  trường hợp "vLLM ở xa" như một target hợp lệ.
- **Production gap #7 — Spark Connect và vLLM ngoài không phát OTel span dưới
  tên service riêng.** `test_the_trace_spans_the_processes_the_contract_claims`
  thất bại vì một trace đầy đủ chỉ thấy 3 service name
  (`lab28-airflow`, `lab28-api`, `lab28-gateway`) thay vì ≥4: Spark Connect
  không được instrument OTel, và vLLM (server ngoài, không thuộc stack OTel
  của mình) không tự gắn `service.name` riêng — span `lab28.spark.delta_merge`
  và `lab28.vllm.chat_completion` đều được ghi nhận (đủ tên span, xem IP10)
  nhưng dưới `service.name=lab28-api` (phía gọi), không phải một service độc
  lập như kiến trúc 10-điểm-kết-nối ngụ ý.

Ba gap #5–#7 chỉ lộ ra khi có vLLM thật để chạy đủ bộ test gắn marker `gpu`;
đã không sửa `gateway/envoy.yaml` hay cấu hình Prometheus vì ngoài phạm vi
được giao ban đầu (`integration_tasks.py`), giữ nguyên làm bằng chứng gap thật
thay vì che giấu.

- **Production gap #8 — chưa demo live K8s/GitOps drift-rollback trên máy này.**
  `kubectl` có sẵn (đi kèm Docker Desktop) nhưng Kubernetes của Docker Desktop
  chưa bật, và RAM chỉ còn ~2.3 GB trống khi `--profile full` đã chạy (từng
  crash 2 lần ở mức RAM tương tự — xem sự cố #1) — bật thêm control plane
  (etcd/api-server/scheduler/kubelet) lúc này là rủi ro không đáng, nên chỉ
  dừng ở `uv run python scripts/validate_manifests.py` (pass: non-root, không
  `:latest`, `targetRevision` pinned, đủ `Deployment/Service/HPA/PDB/
  NetworkPolicy/Gateway/HTTPRoute`). Live drift/rollback (Argo CD tự đồng bộ
  lại khi ai đó `kubectl edit` trực tiếp) cần một cụm K8s riêng — làm ở máy
  nhóm/giảng viên cấp khi có, không ép trên máy đang gần hết RAM.
- **Sự cố thật #2 — Kaggle "Save & Run All" tắt mất phiên GPU giữa demo.**
  Lần đầu nối vLLM, dùng nút "Save Version → Save & Run All (Commit)" để chạy
  notebook — tưởng đây là cách "khởi động" server. Thực tế chế độ này chạy
  hết mọi cell rồi Kaggle **tắt luôn phiên GPU ngay sau đó** (đúng chức năng
  của nó: chụp lại snapshot kết quả, không phải giữ server sống). Hậu quả: URL
  tunnel vừa lấy được đã chết, `/version` trả 502 Bad Gateway ngay khi kiểm
  tra. Phát hiện qua log Cell 5 (`vllm.log`) cho thấy `vllm serve` từng chạy
  thành công rồi biến mất. Khắc phục: chuyển sang chạy **tương tác** (từng ô
  `Shift+Enter`, không Save & Run All, giữ tab mở) — vLLM sống ổn định sau đó.
  Bài học: một quy trình "tưởng đã chạy xong (7 cell)" có thể là dấu hiệu sai
  chế độ chạy, không phải thành công — luôn xác minh bằng cách gọi thật vào
  endpoint (`/version`, `/v1/models`) từ bên ngoài trước khi tin.

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
  liệu vào Qdrant (17 điểm); nối vLLM thật qua Kaggle T4 (xử lý sự cố
  `torchcodec`, mở tunnel `cloudflared`), xác minh `/version`/`/v1/models`/
  `vllm:` metric — IP07 chuyển từ `not_ready` sang `ready`.
- **Platform & Observability (IP08–IP10):** cài `readiness_status`; đo Envoy
  rate limit thật dưới tải (30 request, 11/19); xác nhận toàn bộ Prometheus
  target `up` và dashboard Grafana; thu đủ 11/11 span bắt buộc qua Jaeger; sửa
  `lab28 seed` để retry khi bị 429 thay vì báo lỗi giả.
- **Presenter:** biên soạn tài liệu này, thứ tự demo (mục 5), và mục Q&A
  (mục 6); ghi lại sự cố Docker Desktop và cách khôi phục làm bằng chứng cho
  phần "một sự cố → khôi phục → không mất dữ liệu" của demo.

## 5. Thứ tự demo đề xuất

1. Kiến trúc + 10 điểm kết nối (bảng ở mục 1) — đủ 10/10 điểm có bằng chứng
   sống, kể cả IP07 (vLLM thật qua Kaggle T4).
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
7. `ready`/`degraded`/`not_ready`: `curl http://localhost:8000/ready` → `ready`
   khi vLLM sống, `not_ready` khi tunnel Kaggle rớt (đã thấy thật khi notebook
   bị "Save & Run All" tắt phiên GPU giữa chừng) — đối chiếu với
   `readiness_status` trong `integration_tasks.py`.
8. MLflow promotion/rollback trực tiếp: `uv run lab28 release --prompt-version v2`
   (v3 → v6, champion mới) → gọi `/api/v1/ask`, thấy `mlflow_release_version: 6`
   → `uv run lab28 rollback` (v6 → v5) → gọi lại `/ask`, thấy
   `mlflow_release_version: 5` — không sửa một dòng code nào giữa hai lần gọi.
   Bằng chứng: `evidence/ip06-promotion-rollback.json`.
9. K8s/GitOps: `uv run python scripts/validate_manifests.py` (pass), giải
   thích các ràng buộc (non-root, không `:latest`, `targetRevision` pinned).
10. Giới hạn còn lại: 3 test gắn marker `gpu` thất bại (Production gap #5–#7 ở
    mục 3) — gateway chưa tự loại pod `not_ready`, Prometheus scrape target
    giả định GPU local, Spark/vLLM chưa có OTel service riêng. Giải thích đây
    là giới hạn hạ tầng thật, không phải lỗi của phần được giao.
11. Q&A.

## 6. Chuẩn bị Q&A

**IP07 nối vLLM thật bằng cách nào?**
Kaggle Notebook, GPU T4 x2, `pip install vllm==0.26.0`, `vllm serve
Qwen/Qwen3-1.7B`. Vì Kaggle không cho inbound HTTP trực tiếp, dùng
`cloudflared tunnel --url http://127.0.0.1:8000` (quick tunnel, không cần tài
khoản) để lấy URL public tạm thời, rồi trỏ `LAB28_VLLM_BASE_URL` của container
`api` vào đó và `LAB28_VLLM_REQUIRE_REAL=true`. Gate IP07 kiểm tra `/version`
thật của vLLM và metric `vllm:` — một server giả bắt chước OpenAI API sẽ fail
ngay vì không có hai tín hiệu này.

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
Sửa 3 gap hạ tầng lộ ra sau khi có vLLM thật (Production gap #5–#7): thêm
`outlier_detection` vào cluster `api` trong `gateway/envoy.yaml` để gateway tự
loại pod `not_ready`; thêm một scrape job Prometheus theo biến môi trường cho
trường hợp vLLM chạy ở xa (không chỉ `host.docker.internal:8001`); và
instrument OTel cho Spark Connect để span `lab28.spark.delta_merge` đứng dưới
service riêng thay vì gộp vào `lab28-api`.

**Endpoint Kaggle có ổn định để dùng lâu dài không?**
Không — đây là giới hạn đã ghi trong `KAGGLE_GPU_EXTENSION.md`: quick tunnel
`trycloudflare.com` chết theo phiên notebook, notebook tự ngắt sau một thời
gian idle, và quota GPU tuần có thể hết giữa buổi demo. Trong lúc làm bài, tunnel
đã chết **hai lần** (một lần do "Save & Run All", một lần do notebook tự ngắt
idle) — đây là bằng chứng cho Production gap #1 (đã đóng cho mục đích demo,
nhưng không phải giải pháp production — production cần một endpoint vLLM ổn
định, có SLA, không phụ thuộc một notebook đang mở).

## 7. Reflection

**Điều khó nhất:** không phải viết 4 hàm TODO (thẳng, có test dẫn đường), mà
là **chẩn đoán đúng lớp gây lỗi** khi một thứ không hoạt động trên một máy vừa
chạy nhiều dự án khác vừa giới hạn RAM — ví dụ tunnel Kaggle trả 502 có thể do
tunnel chết, vLLM chết, hay notebook mất session, và mỗi nguyên nhân cần một
cách kiểm tra khác nhau (xem log `vllm.log` bên trong notebook, so `docker
compose ps`, hay gọi thẳng endpoint từ ngoài). Khó thứ hai là phân biệt "lỗi
do mình" với "giới hạn hạ tầng có thật" (3 test gắn marker `gpu` thất bại) —
phải đọc kỹ code (`gateway/envoy.yaml`, cấu hình Prometheus) để chứng minh đó
là gap kiến trúc, không phải bug của 4 hàm được giao.

**Trade-off đã chọn:** ưu tiên **evidence trung thực hơn điểm số đẹp** xuyên
suốt — báo `UNVERIFIED`/`not_ready` thật khi vLLM chưa nối hoặc Delta chưa có
dữ liệu, thay vì dựng giả để mọi thứ "xanh". Cụ thể: `evidence/ip04-feast-online.json`
từng lưu nguyên trạng thái `NOT_FOUND` thay vì xoá đi khi Feast chưa
materialize; `evidence/ip07-vllm-identity.json` từng lưu `is_real_vllm: false`
thay vì bỏ qua. Đổi lại, quá trình chậm hơn (phải chạy lại nhiều lần, chờ
Docker/Kaggle) nhưng bằng chứng cuối cùng tái lập được và không ai phải tin
lời nói suông.

**Điều sẽ cải tiến nếu làm lại:** (1) dựng sẵn Kaggle notebook + cloudflared +
gỡ torchcodec thành một cell "setup" duy nhất chạy được ngay lần đầu, tránh
mất thời gian dò lỗi `OSError`/`FileNotFoundError` qua nhiều lượt; (2) kiểm tra
RAM trống *trước khi* chạy `--profile full` và đóng ứng dụng không cần thay vì
để nó crash rồi mới xử lý; (3) thêm `outlier_detection` vào Envoy và một scrape
job Prometheus theo biến môi trường ngay từ đầu, để không phải chờ có vLLM
thật mới phát hiện ra 2 gap đó.
