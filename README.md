# Airflow Mastery — Lộ trình làm chủ Apache Airflow cho Data Engineer

> Project tự học có hệ thống, đi từ kiến trúc → giao diện → lập trình DAG → debug/giám sát → tích hợp hệ sinh thái dữ liệu → vận hành production.
> Phiên bản tham chiếu: **Apache Airflow 3.2.x** (bản ổn định mới nhất tính đến 06/2026). Toàn bộ code, import path, docker-compose trong project này đều theo kiến trúc Airflow 3, **không phải Airflow 2** — đây là điểm rất nhiều tài liệu/blog cũ trên mạng còn nhầm lẫn, sẽ giải thích kỹ ở Module 0.

---

## 1. Triết lý của lộ trình này

Airflow không khó vì cú pháp Python — nó khó vì là một **hệ phân tán nhiều thành phần** (scheduler, processor, worker, database, queue) phối hợp với nhau qua trạng thái lưu trong DB. Nếu chỉ học "cách viết DAG" mà không hiểu *điều gì xảy ra phía sau* khi bấm nút Trigger, bạn sẽ mãi mãi loay hoay khi DAG "không chạy" mà không biết bắt đầu debug từ đâu.

Vì vậy lộ trình này đi theo thứ tự: **Hiểu kiến trúc → Quan sát qua UI → Tự tay viết → Làm hỏng nó để học cách debug → Nối nó với hệ thống thật → Vận hành nó ở quy mô lớn.** Mỗi module đều có 3 phần: lý thuyết có ví dụ thực tế, DAG mẫu chạy được, và bài tập để bạn tự làm trên môi trường Docker đã dựng sẵn.

## 2. Cấu trúc project

```
airflow-mastery/
├── README.md                         # File này — bản đồ tổng của lộ trình
├── docker-compose.yaml               # Môi trường Airflow 3.x local (CeleryExecutor)
├── .env                               # Biến môi trường cho docker-compose
├── dags/
│   └── module_00_kien_truc_va_hello_world/
│       ├── hello_world_classic_dag.py     # DAG kiểu "classic" với Operator
│       ├── hello_world_taskflow_dag.py    # DAG kiểu TaskFlow (@dag/@task)
│       └── bash_etl_pipeline_dag.py       # Mô phỏng pipeline ETL 3 bước
├── docs/
│   ├── 00_kien_truc_va_khai_niem_cot_loi.md
│   └── 01_cai_dat_va_kham_pha_giao_dien.md
├── logs/                              # Log của các lần chạy task (tự sinh)
├── plugins/                           # Nơi đặt custom operator/hook sau này
└── config/                            # File cấu hình airflow.cfg tuỳ biến (nếu cần)
```

Quy ước: mỗi module mới sẽ có một thư mục `dags/module_XX_ten_module/` và một file lý thuyết tương ứng trong `docs/`. Bạn cứ thêm dần khi đi qua từng module — đừng tải hết về một lúc rồi bỏ đó.

## 3. Yêu cầu môi trường

- Docker Desktop / Docker Engine + Docker Compose v2.14+
- Tối thiểu 4 GB RAM cấp cho Docker, khuyến nghị 8 GB nếu chạy đủ Celery worker + Redis + Postgres
- Tối thiểu 10 GB dung lượng đĩa trống

## 4. Lộ trình chi tiết — 11 module

| # | Module | Nội dung chính | Kỹ năng đạt được | Trạng thái |
|---|--------|-----------------|---------------------|------------|
| 0 | **Kiến trúc & khái niệm cốt lõi** | API Server, Scheduler, DAG Processor, Triggerer, Worker, Metadata DB; vòng đời DAG/Task/DagRun/TaskInstance; `logical_date`, `data_interval` | Đọc hiểu được Airflow "nói chuyện" với nhau như thế nào trước khi đụng vào code | ✅ Có sẵn trong project |
| 1 | **Cài đặt môi trường & khám phá giao diện** | Dựng Airflow bằng Docker Compose; tour toàn bộ UI: Grid, Graph, Calendar, Gantt, Code, Audit Log, Admin menu | Tự dựng môi trường local, đọc hiểu mọi màn hình trong UI | ✅ Có sẵn trong project |
| 2 | **Viết DAG đầu tiên** | `DAG`, `BashOperator`, `PythonOperator`, `default_args`, `schedule`, `start_date`, `catchup`, backfill | Viết được DAG tuyến tính hoàn chỉnh, hiểu rõ lịch chạy | ⏳ Lộ trình tiếp theo |
| 3 | **TaskFlow API hiện đại** | `@dag`, `@task`, truyền dữ liệu qua XCom tự động, type hint, `.override()` | Viết DAG theo phong cách Python thuần, sạch, dễ test | ⏳ Lộ trình tiếp theo |
| 4 | **Luồng điều khiển nâng cao** | XCom thủ công, Branching (`@task.branch`), TaskGroup, Dynamic Task Mapping (`.expand()`) | Xây pipeline có rẽ nhánh và sinh task động theo dữ liệu | ⏳ Lộ trình tiếp theo |
| 5 | **Sensor, Trigger & Asset-aware scheduling** | `Sensor`, deferrable operator, `trigger_rule`, lập lịch theo Asset (thay cho Dataset cũ) | Xây pipeline phản ứng theo sự kiện/dữ liệu thay vì chỉ theo giờ | ⏳ Lộ trình tiếp theo |
| 6 | **Connections, Hooks, Variables & Secrets** | Quản lý kết nối tới hệ thống ngoài, Hook, Variable, Secrets Backend (Vault/AWS Secrets Manager) | Kết nối Airflow với DB/API ngoài một cách an toàn, không hardcode | ⏳ Lộ trình tiếp theo |
| 7 | **Giám sát & Debug chuyên sâu trên UI** | Đọc log lỗi, tab Rendered Template/XCom/Details, Clear, Mark Success/Failed, Trigger kèm config JSON, Backfill, Deadline Alert | Tự debug một DAG lỗi trong môi trường thật mà không cần hỏi ai | ⏳ Lộ trình tiếp theo |
| 8 | **Tích hợp hệ sinh thái dữ liệu** | Postgres/MySQL, S3/MinIO/GCS, Spark, dbt, Snowflake/BigQuery, Kafka, cảnh báo Slack/Email | Biến Airflow thành trung tâm điều phối thật của một hệ data platform | ⏳ Lộ trình tiếp theo |
| 9 | **Kiểm thử & CI/CD cho DAG** | Unit test DAG bằng `pytest`, `dag.test()`, DAG Versioning, idempotency, checklist review code | Đưa DAG qua pipeline CI/CD như mọi phần mềm khác, không "deploy bằng tay" | ⏳ Lộ trình tiếp theo |
| 10 | **Vận hành ở quy mô Production** | Scaling Executor (Celery/Kubernetes), Pool, Priority Weight, mô hình bảo mật multi-tenant, giám sát bằng OpenTelemetry/Prometheus | Vận hành cụm Airflow phục vụ nhiều team, nhiều pipeline cùng lúc | ⏳ Lộ trình tiếp theo |

## 5. Cách dùng project này

1. Đọc `docs/00_kien_truc_va_khai_niem_cot_loi.md` trước khi mở bất kỳ dòng code nào — đây là nền tảng cho mọi thứ phía sau.
2. Làm theo `docs/01_cai_dat_va_kham_pha_giao_dien.md` để dựng môi trường và làm quen UI.
3. Mở 3 DAG mẫu trong `dags/module_00_kien_truc_va_hello_world/` trên UI, chạy thử, đọc log, đối chiếu với lý thuyết.
4. Báo lại để mình build tiếp Module 2 (Viết DAG đầu tiên) — mỗi module sau sẽ được thêm vào đúng cấu trúc thư mục này, có lý thuyết + DAG mẫu + bài tập, để bạn đi từng bước một thay vì ngợp vì lượng kiến thức.

## 6. Gợi ý nhịp độ học

Nếu học song song với công việc full-time: ~1 module/tuần là vừa sức, vị chi khoảng 11 tuần để đi hết toàn bộ lộ trình và có thể tự tin dùng Airflow trong công việc thực tế (cả viết DAG lẫn vận hành/debug trên UI).
