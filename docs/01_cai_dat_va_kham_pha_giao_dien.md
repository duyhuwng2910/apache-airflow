# Module 1 — Cài đặt môi trường & Khám phá giao diện Airflow

## 1.1. Dựng môi trường bằng Docker Compose

File `docker-compose.yaml` đi kèm project này dựng đầy đủ các thành phần đã học ở Module 0: `postgres` (metadata DB), `redis` (hàng đợi cho CeleryExecutor), `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`. Đây là bản dựng chuẩn theo kiến trúc Airflow 3.x, gần với cách Airflow thực sự chạy ở môi trường production (chỉ khác là production sẽ scale nhiều worker hơn và không chạy mọi thứ trên một máy).

Các bước chạy (thực hiện trong thư mục `airflow-mastery/`):

```bash
# 1. Tạo các thư mục cần thiết và set quyền user (chỉ cần trên Linux/macOS)
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# 2. Khởi tạo database + tài khoản admin lần đầu
docker compose up airflow-init

# 3. Chạy toàn bộ cụm
docker compose up -d

# 4. Theo dõi trạng thái các container — đợi tới khi tất cả "healthy"
docker compose ps
```

Sau khi mọi container ở trạng thái `healthy`, mở trình duyệt tới `http://localhost:8080`. Tài khoản mặc định: `airflow` / `airflow` (đã khai báo trong `.env`, **nhớ đổi khi dùng thật**).

Vài lệnh hữu ích khi vận hành:

```bash
docker compose logs -f airflow-scheduler      # xem log scheduler theo thời gian thực
docker compose logs -f airflow-dag-processor  # xem log quá trình parse DAG — chỗ đầu tiên cần nhìn khi DAG "không hiện"
docker compose down                           # tắt cụm, giữ lại dữ liệu (volume)
docker compose down --volumes                 # tắt và xoá sạch dữ liệu — dùng khi muốn làm lại từ đầu
```

## 1.2. Đối chiếu file docker-compose.yaml với kiến trúc đã học

Mở file `docker-compose.yaml`, bạn sẽ thấy mỗi service tương ứng đúng một ô trong sơ đồ kiến trúc ở Module 0:

- `airflow-apiserver` chạy lệnh `api-server` → chính là "phòng điều hành trung tâm", expose cổng `8080`.
- `airflow-scheduler` chạy lệnh `scheduler` → "quản đốc", không có quyền đọc file DAG.
- `airflow-dag-processor` chạy lệnh `dag-processor` → bộ phận đọc và parse file `.py` trong `dags/`.
- `airflow-worker` chạy lệnh `celery worker` → công nhân thực thi task, dùng CeleryExecutor (hàng đợi qua `redis`).
- `airflow-triggerer` chạy lệnh `triggerer` → xử lý task kiểu chờ đợi không tốn slot.
- `postgres` → metadata DB.
- `redis` → hàng đợi (broker) cho CeleryExecutor.

Đây cũng là lý do thư mục `./dags` trên máy bạn được mount **vào cả `airflow-scheduler` lẫn `airflow-dag-processor` lẫn `airflow-worker`** trong file compose — vì DAG Processor cần đọc file để parse, còn Worker cần đọc file để thực sự import và chạy code Python bên trong task.

## 1.3. Tour giao diện UI

### Trang danh sách DAG (`/dags`)

Đây là trang mặc định sau khi đăng nhập. Mỗi dòng là một DAG, các cột quan trọng:

- **Toggle bật/tắt** ở đầu dòng — DAG tắt (paused) sẽ không được Scheduler tạo DagRun mới, dù lịch có tới hạn.
- **Tags** — nhãn để nhóm DAG, lọc nhanh khi có hàng trăm DAG.
- **Schedule** — hiển thị lịch chạy dạng người đọc được.
- **Recent runs** — dải ô màu nhỏ thể hiện trạng thái các lần chạy gần nhất (xanh = thành công, đỏ = thất bại, vàng/cam = đang chạy hoặc upstream failed).
- **Actions** — nút Trigger (▶), nút xoá, link nhanh tới Graph/Grid view.

**Lưu ý quan trọng:** nếu một file DAG có lỗi cú pháp Python hoặc lỗi import, bạn sẽ thấy một **banner đỏ "Import Error"** ở đầu trang này, kèm traceback đầy đủ. Đây là nơi đầu tiên cần nhìn khi DAG bạn vừa thêm "biến mất" hoặc không hoạt động — rất nhiều người mới tìm sai chỗ (đi tìm trong log scheduler) trong khi lỗi parse hiển thị ngay trên UI.

### Grid view (thay cho Tree view ở bản cũ)

Vào một DAG cụ thể, đây là tab mặc định. Trục dọc là các Task, trục ngang là các DagRun theo thời gian — mỗi ô vuông là một TaskInstance, màu sắc thể hiện trạng thái (`success`, `failed`, `running`, `up_for_retry`, `skipped`, `upstream_failed`...). Đây là màn hình bạn sẽ nhìn nhiều nhất trong công việc hàng ngày: chỉ liếc qua là biết pipeline 30 ngày qua chạy ổn không, ngày nào lỗi.

Click vào một ô sẽ mở panel chi tiết của TaskInstance đó với các tab:

- **Log** — log thực thi đầy đủ của lần chạy đó. Khi task failed, đây là nơi đầu tiên bạn xem traceback lỗi thật.
- **Rendered Template** — xem nội dung **sau khi** Jinja template đã được render (rất hữu ích khi dùng `{{ ds }}`, `{{ params }}` — bạn xem được giá trị thật sự được đưa vào lệnh, thay vì đoán).
- **XCom** — xem dữ liệu mà task này đã đẩy ra cho task khác đọc.
- **Details** — thời gian bắt đầu/kết thúc, thời lượng, số lần thử (try number), pool, queue, operator sử dụng.

### Graph view

Hiển thị trực quan cấu trúc phụ thuộc giữa các Task dưới dạng đồ thị — rất hữu ích khi DAG có rẽ nhánh, TaskGroup phức tạp, để hiểu *thứ tự logic* hơn là *lịch sử theo thời gian* (đó là việc của Grid view).

### Calendar view

Bản đồ nhiệt (heatmap) theo ngày, cho biết DagRun của ngày đó thành công/thất bại/một phần. Hữu ích khi cần báo cáo nhanh "tháng vừa rồi pipeline chạy ổn định bao nhiêu %".

### Gantt view

Biểu đồ thanh ngang thể hiện thời lượng từng Task trong một DagRun cụ thể, xếp theo thời gian thực — dùng để tìm **task nào đang là nút thắt cổ chai** làm cả pipeline chạy lâu.

### Code view

Hiển thị nguyên văn source code Python của DAG — tiện để xem nhanh logic mà không cần mở IDE.

### Audit Log

Ghi lại mọi hành động của người dùng trên DAG này: ai trigger, ai clear, ai pause/unpause, vào lúc nào — quan trọng khi vận hành nhóm nhiều người, cần truy vết "ai vừa chạy lại pipeline production".

### Menu Browse (áp dụng toàn hệ thống, không riêng 1 DAG)

- **DAG Runs** — danh sách tất cả lần chạy của mọi DAG, lọc theo trạng thái.
- **Task Instances** — danh sách tất cả TaskInstance, lọc theo DAG/trạng thái/khoảng thời gian — hữu ích để tìm nhanh "tất cả task nào đang failed trong toàn hệ thống ngay bây giờ".
- **Jobs** — trạng thái heartbeat của các tiến trình hệ thống (Scheduler, DAG Processor, Triggerer) — kiểm tra ở đây khi nghi ngờ một thành phần đã "chết" mà container vẫn đang chạy.

### Menu Admin

- **Connections** — nơi khai báo thông tin kết nối tới hệ thống ngoài (MySQL, S3, Slack...) — sẽ dùng nhiều ở Module 6.
- **Variables** — cặp key-value dùng chung cho nhiều DAG, đọc qua `Variable.get()`.
- **Pools** — giới hạn số task chạy đồng thời cho một nhóm tài nguyên (ví dụ giới hạn 5 task cùng lúc gọi vào một API ngoài hay rate-limit).
- **Plugins / Providers** — danh sách provider package đã cài, version.
- **Config** — xem cấu hình `airflow.cfg` hiện hành trực tiếp trên UI.

## 1.4. Các thao tác vận hành/debug cơ bản cần thuộc lòng

| Thao tác | Khi nào dùng |
|---|---|
| **Trigger DAG** (▶, có thể kèm JSON config qua `Params`) | Chạy thử ngay lập tức không cần đợi lịch, có thể truyền tham số tuỳ biến cho lần chạy đó |
| **Pause/Unpause** | Tạm dừng một pipeline đang gây lỗi mà không xoá lịch sử |
| **Clear** | Xoá trạng thái của TaskInstance để Scheduler chạy lại — dùng khi muốn retry sau khi đã sửa lỗi (lưu ý: Clear một task sẽ kéo theo các task downstream phụ thuộc nó cũng được reset) |
| **Mark Success / Mark Failed** | Đánh dấu thủ công trạng thái mà *không* thực sự chạy lại — dùng khi bạn biết task đó thực ra đã xong (hoặc cố tình bỏ qua) ngoài luồng tự động |
| **Backfill** | Yêu cầu chạy lại một khoảng `logical_date` trong quá khứ — qua CLI (`airflow dags backfill`), qua `airflowctl`, hoặc qua UI |

## 1.5. Lỗi thường gặp khi mới dựng môi trường

- **DAG không hiện trên UI dù file đã đúng cú pháp**: kiểm tra log của `airflow-dag-processor` trước — có thể DAG Processor chưa kịp quét (mặc định 5 phút/lần) hoặc đang lỗi.
- **DagRun không bao giờ được tạo dù đã unpause**: kiểm tra `start_date` của DAG — nếu `start_date` ở tương lai, sẽ không có lần chạy nào. Kiểm tra thêm `catchup` nếu mong đợi chạy bù quá khứ.
- **Container `airflow-init` chạy xong rồi thoát (exit code 0)** — đây là hành vi đúng, `airflow-init` chỉ chạy migration DB và tạo user một lần rồi dừng, không phải lỗi.
- **Cảnh báo "AIRFLOW_UID not set"** trên Linux — chạy lại lệnh `echo -e "AIRFLOW_UID=$(id -u)" >> .env` ở bước 1.

## 1.6. Bài tập cuối module

1. Dựng cụm Airflow bằng `docker-compose.yaml` trong project, đăng nhập UI thành công.
2. Bật 3 DAG mẫu trong `dags/module_00_kien_truc_va_hello_world/`, Trigger thủ công từng DAG, vào Grid view xem kết quả.
3. Cố tình sửa sai một dòng trong `bash_etl_pipeline_dag.py` (ví dụ gõ sai lệnh bash) để task fail, sau đó thực hành: đọc log lỗi → sửa file → **Clear** task đó → xem nó tự chạy lại thành công.
4. Vào tab **Rendered Template** của một task có dùng `{{ ds }}` để xem Jinja đã render ra giá trị gì.

Khi sẵn sàng, báo lại để mình build Module 2 — nơi bạn sẽ tự viết DAG đầu tiên từ đầu, hiểu sâu về lịch chạy (`schedule`), `catchup` và backfill.
