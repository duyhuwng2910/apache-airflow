# Module 0 — Kiến trúc & Khái niệm cốt lõi của Apache Airflow

## 0.1. Airflow giải quyết vấn đề gì?

Trước Airflow, dân kỹ thuật dữ liệu quản lý pipeline bằng cron job. Vấn đề của cron: nó không biết task A có chạy xong chưa trước khi chạy task B; nó không có chỗ xem lịch sử chạy thành công/thất bại; nó không tự retry; nó không cho bạn biết task nào đang chậm. Khi pipeline có 5 bước phụ thuộc nhau, bạn phải tự viết logic chờ đợi, tự viết log, tự viết cảnh báo — và làm lại việc đó cho từng pipeline mới.

Airflow giải quyết đúng vấn đề này: bạn khai báo pipeline dưới dạng một **đồ thị có hướng không chu trình** (Directed Acyclic Graph — DAG), Airflow lo phần còn lại: chạy đúng thứ tự, lưu lịch sử, retry, cảnh báo, hiển thị trực quan.

## 0.2. Phép ẩn dụ: Airflow như một nhà máy sản xuất

Hãy hình dung Airflow như một nhà máy có dây chuyền sản xuất:

- **DAG** giống như **bản thiết kế quy trình sản xuất** — quy định công đoạn nào làm trước, công đoạn nào làm sau.
- **Task / Operator** là **từng trạm trên dây chuyền** — mỗi trạm làm một việc cụ thể (cắt, hàn, sơn...).
- **DAG Processor** là **bộ phận đọc bản thiết kế mới** mỗi khi có bản vẽ cập nhật, rồi lưu vào "kho thiết kế đã duyệt" (bảng `serialized_dag` trong metadata DB).
- **Scheduler** là **quản đốc** — không tự đọc bản vẽ gốc, mà chỉ nhìn vào kho thiết kế đã duyệt để quyết định: hôm nay lô hàng nào cần sản xuất, công đoạn nào đã đủ điều kiện bắt đầu.
- **Worker** là **công nhân thực sự đứng máy**, nhận lệnh từ quản đốc và thực thi từng trạm.
- **API Server** là **phòng điều hành trung tâm**: vừa hiển thị giao diện theo dõi (UI) cho người quản lý, vừa là nơi duy nhất công nhân (worker) được phép báo cáo tiến độ hoặc xin dữ liệu — công nhân **không được phép tự ý vào kho dữ liệu gốc** (metadata DB) mà phải đi qua phòng điều hành. Đây là thay đổi kiến trúc lớn nhất của Airflow 3 so với Airflow 2.
- **Triggerer** là bộ phận chuyên xử lý các công đoạn "chờ đợi dài hạn không tốn nhân lực" (ví dụ chờ một file xuất hiện) mà không cần giữ một công nhân đứng chờ không làm gì.
- **Metadata Database** (thường là PostgreSQL) là **kho lưu trữ trung tâm**: toàn bộ lịch sử, trạng thái, kết quả đều nằm ở đây.

## 0.3. Kiến trúc Airflow 3.x — vì sao quan trọng phải hiểu đúng bản mới nhất

Rất nhiều tài liệu, video, blog hướng dẫn Airflow trên mạng vẫn mô tả kiến trúc Airflow 2.x, trong đó chỉ có 3 tiến trình chính: `webserver`, `scheduler`, `worker` — và `scheduler` tự đọc luôn file DAG. Từ Airflow 3.0 (và hiện tại là bản ổn định 3.2), kiến trúc đã tách nhỏ hơn để tăng bảo mật và khả năng mở rộng:

```
            ┌────────────────┐
   Bạn -->  │   API Server   │ <----> UI người dùng (React, FastAPI-based)
            │ (thay Webserver)│        REST API v2
            └───────┬────────┘
                     │ đọc/ghi trạng thái
            ┌────────┴────────┐
            │  Metadata DB    │ (PostgreSQL)
            │ (serialized_dag,│
            │  dag_run, ...)  │
            └───┬────────┬────┘
                │         │
       ghi DAG  │         │ đọc DAG đã parse
      đã parse  │         │
   ┌────────────┴──┐   ┌──┴───────────┐
   │ DAG Processor  │   │  Scheduler   │
   │ (đọc file .py  │   │ (KHÔNG đọc   │
   │  trong dags/,  │   │  file DAG    │
   │  parse, lưu    │   │  trực tiếp   │
   │  vào DB)       │   │  nữa!)       │
   └────────────────┘   └──────┬───────┘
                                │ đẩy task vào hàng đợi
                         ┌──────┴───────┐
                         │ Worker(s)    │ -- gọi API Server để
                         │ (Celery/K8s) │    xin Connection, báo
                         └──────────────┘    trạng thái, ghi XCom

                         ┌──────────────┐
                         │  Triggerer   │ -- xử lý deferrable
                         │ (async loop) │    operator/sensor
                         └──────────────┘
```

Điểm mấu chốt cần nhớ:

1. **Scheduler không còn tự đọc file DAG.** Việc đó do **DAG Processor** đảm nhiệm — nó đọc thư mục `dags/`, parse từng file `.py`, rồi ghi kết quả đã "đóng gói" (serialize) vào bảng `serialized_dag` trong metadata DB. Nếu bạn chạy `scheduler` mà quên chạy `dag-processor`, scheduler sẽ khởi động sạch sẽ nhưng **không bao giờ thấy DAG nào cả** — đây là lỗi rất hay gặp khi tự dựng Airflow 3 thủ công.
2. **Worker không còn quyền truy cập trực tiếp metadata DB.** Mọi thao tác — báo trạng thái task, ghi/đọc XCom, xin thông tin Connection — đều phải đi qua **Task Execution API** trên API Server, dùng token JWT ngắn hạn. Lợi ích: code task của bạn (do bạn viết, có thể chứa lỗi hoặc thậm chí mã độc nếu không kiểm soát) không bao giờ có đường chạm trực tiếp vào dữ liệu hệ thống.
3. **Webserver đã đổi tên/bản chất thành API Server**: vừa phục vụ giao diện UI (xây trên React + FastAPI) vừa expose REST API v2 cho lập trình viên gọi từ bên ngoài.
4. **Triggerer** chỉ cần thiết nếu bạn dùng sensor/operator kiểu "deferrable" (ví dụ chờ sự kiện mà không chiếm slot worker liên tục) — môi trường đơn giản có thể không cần.

## 0.4. Các khái niệm cốt lõi bạn sẽ dùng hàng ngày

| Khái niệm | Giải thích | Ví dụ thực tế |
|---|---|---|
| **DAG** | Bản khai báo pipeline — tập hợp Task và quan hệ phụ thuộc giữa chúng | "Pipeline xử lý đơn hàng hàng ngày" |
| **Task** | Một đơn vị công việc trong DAG | "Trích xuất dữ liệu từ MySQL" |
| **Operator** | Khuôn mẫu có sẵn để tạo Task (đã viết sẵn logic, bạn chỉ truyền tham số) | `BashOperator`, `PythonOperator` |
| **DagRun** | Một lần thực thi cụ thể của cả DAG, gắn với một khoảng thời gian dữ liệu | "Lần chạy pipeline cho ngày 2026-06-20" |
| **TaskInstance** | Một lần thực thi cụ thể của một Task, trong một DagRun | "Task trích xuất dữ liệu, của lần chạy ngày 2026-06-20, lần thử thứ 2" |
| **`logical_date`** | Thời điểm dữ liệu mà DagRun đại diện cho (KHÔNG phải giờ thực tế DAG chạy) — tên mới thay cho `execution_date` đã bị loại bỏ ở Airflow 3 | DAG lên lịch `@daily`, chạy lúc 00:05 ngày 21/6 nhưng xử lý dữ liệu của ngày 20/6 → `logical_date` = 20/6 |
| **`data_interval`** | Khoảng thời gian dữ liệu mà DagRun chịu trách nhiệm xử lý (start → end) | `data_interval_start = 2026-06-20 00:00`, `data_interval_end = 2026-06-21 00:00` |
| **`schedule`** | Tần suất chạy DAG, khai báo bằng cron string, preset (`@daily`) hoặc Timetable — tên tham số mới thay cho `schedule_interval` cũ | `schedule="0 6 * * *"` (6h sáng mỗi ngày) |
| **`catchup`** | Có chạy bù các lần chạy trong quá khứ (từ `start_date` đến hiện tại) hay không khi DAG mới được bật | `catchup=False` → chỉ chạy từ thời điểm hiện tại trở đi |
| **Backfill** | Hành động chủ động yêu cầu Airflow chạy lại các DagRun cho một khoảng thời gian trong quá khứ | Chạy bù dữ liệu của cả tháng 5 sau khi sửa lỗi pipeline |

## 0.5. Ví dụ thực tế để hình dung trọn vẹn

Giả sử bạn là Data Engineer phụ trách: mỗi ngày lúc 6h sáng, hệ thống cần lấy dữ liệu đơn hàng của ngày hôm trước từ MySQL, làm sạch, nạp vào Data Warehouse (Postgres), rồi báo qua Slack khi xong.

- Bạn viết file `daily_orders_pipeline.py` khai báo DAG này, đặt vào thư mục `dags/`.
- **DAG Processor** phát hiện file mới (mặc định quét mỗi 5 phút, có thể chỉnh `[dag_processor] refresh_interval`), parse và lưu bản "đã duyệt" vào metadata DB.
- **Scheduler** nhìn vào lịch (`schedule="0 6 * * *"`), thấy đã đến giờ, tạo một **DagRun** mới với `logical_date` là ngày hôm qua, đẩy Task đầu tiên ("Extract") vào hàng đợi.
- **Worker** rảnh sẽ nhặt Task này, gọi **API Server** để xin thông tin Connection tới MySQL (đã khai báo sẵn trong Admin → Connections), chạy code, rồi báo kết quả trạng thái + đẩy dữ liệu trung gian qua **XCom** (cũng đi qua API Server).
- Khi Task "Extract" thành công, **Scheduler** thấy điều kiện phụ thuộc của Task "Transform" đã thoả, tiếp tục đẩy nó vào hàng đợi — cứ thế cho đến Task "Notify Slack".
- Toàn bộ quá trình này, bạn quan sát theo thời gian thực qua **API Server** (giao diện UI ở `http://localhost:8080`).

## 0.6. Lưu ý nếu bạn từng học Airflow 2.x từ trước

Nếu bạn từng đọc tài liệu Airflow 2, một số thứ đã thay đổi ở Airflow 3 mà bạn cần "cập nhật lại" trong đầu:

- `execution_date` đã bị xoá khỏi context — dùng `logical_date` (và cẩn thận: khi trigger DAG thủ công, `data_interval` **không còn mặc định bằng** `logical_date` như trước).
- `schedule_interval` đổi tên thành `schedule`.
- `BashOperator`, `PythonOperator` và nhiều operator/sensor cơ bản khác **không còn nằm trong `airflow-core`** nữa mà tách ra package riêng `apache-airflow-providers-standard`. Import path mới: `from airflow.providers.standard.operators.bash import BashOperator`.
- SubDAG bị loại bỏ hẳn — dùng TaskGroup hoặc Asset thay thế.
- SLA (Service Level Agreement) cũ bị loại bỏ, thay bằng cơ chế **Deadline Alert**.
- Dataset (lập lịch theo dữ liệu) được đổi tên thành **Asset**, và Airflow 3.2 vừa bổ sung khả năng "Asset Partitioning" — lập lịch theo từng phần dữ liệu cụ thể (ví dụ một thư mục ngày cụ thể trên S3) thay vì toàn bộ Asset.

## 0.7. Bài tập nhỏ cuối module

Trước khi sang Module 1, hãy tự trả lời (không cần viết ra, chỉ cần chắc chắn bạn giải thích được):

1. Nếu bạn thêm một DAG mới vào thư mục `dags/` nhưng không thấy nó xuất hiện trên UI sau 10 phút, thành phần nào trong kiến trúc bạn nên kiểm tra log đầu tiên?
2. Vì sao Task code (chạy trên Worker) không được phép tự kết nối thẳng vào metadata DB ở Airflow 3, trong khi Airflow 2 thì được?
3. Phân biệt `logical_date` và thời điểm thực tế Airflow chạy task — cho một ví dụ cụ thể như mục 0.4.

Sang Module 1, bạn sẽ tự tay dựng môi trường này bằng Docker Compose và đối chiếu trực tiếp những gì vừa học với màn hình thật.
