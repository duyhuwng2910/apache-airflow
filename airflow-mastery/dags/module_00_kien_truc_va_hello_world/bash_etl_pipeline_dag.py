"""
Module 0 - DAG mẫu #3: Mô phỏng pipeline ETL Extract -> Transform -> Load.

Mục tiêu: một DAG có ý nghĩa thực tế hơn (dù vẫn rất đơn giản) để bạn dùng
làm "vật thí nghiệm" cho bài tập debug ở Module 1 (docs/01_...).

Pipeline mô phỏng: trích xuất dữ liệu đơn hàng (giả lập) -> làm sạch/biến đổi
-> nạp vào kho dữ liệu (ở đây chỉ là ghi ra file trong /tmp để không cần
phụ thuộc hệ thống ngoài nào — phần kết nối hệ thống thật sẽ học ở Module 6/8).

GỢI Ý BÀI TẬP DEBUG (xem chi tiết ở docs/01_cai_dat_va_kham_pha_giao_dien.md):
Hãy thử cố tình gõ sai một lệnh bash bên dưới (ví dụ đổi `cat` thành `cats`)
để task fail, Trigger DAG, quan sát:
  1. Banner đỏ Import Error có xuất hiện không? (sẽ KHÔNG, vì đây là lỗi
     runtime chứ không phải lỗi cú pháp/parse — phân biệt 2 loại lỗi này
     là một kỹ năng debug quan trọng)
  2. Task chuyển sang màu đỏ (failed) trên Grid view.
  3. Mở tab Log của task đó để đọc traceback thật.
  4. Sửa lại file, lưu, đợi DAG Processor parse lại (hoặc khoan, vì sửa nội
     dung bash_command không đổi cấu trúc DAG nên không cần đợi lâu).
  5. Bấm Clear trên task đã fail để Scheduler chạy lại nó.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Thư mục dùng chung để các task "giao tiếp" qua file - cách làm đơn giản
# nhất để minh hoạ ETL nhiều bước mà chưa cần học XCom/Connection.
# Lưu ý: cách này CHỈ phù hợp cho demo - trong thực tế nên dùng XCom (dữ liệu
# nhỏ) hoặc một storage trung gian thật như S3/Postgres (dữ liệu lớn) thay vì
# ghi file cục bộ, vì các task có thể chạy trên các Worker vật lý khác nhau.
WORKDIR = "/tmp/module00_etl_demo"

with DAG(
    dag_id="module00_bash_etl_pipeline",
    description="Mô phỏng pipeline ETL 3 bước (Extract -> Transform -> Load) bằng BashOperator",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["module-00", "etl", "bash"],
    default_args={
        "owner": "airflow-mastery",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
    },
) as dag:

    extract = BashOperator(
        task_id="extract_orders",
        bash_command=(
            f"mkdir -p {WORKDIR} && "
            f'echo \'[{{"order_id": 1001, "amount": 250000}}, '
            f'{{"order_id": 1002, "amount": 480000}}]\' '
            f"> {WORKDIR}/raw_orders_{{{{ ds_nodash }}}}.json && "
            f"echo 'Extract xong, ngày dữ liệu: {{{{ ds }}}}'"
        ),
    )

    transform = BashOperator(
        task_id="transform_orders",
        bash_command=(
            f"cat {WORKDIR}/raw_orders_{{{{ ds_nodash }}}}.json "
            f"> {WORKDIR}/transformed_orders_{{{{ ds_nodash }}}}.json && "
            f"echo 'Transform xong (demo: copy nguyên trạng để minh hoạ luồng chạy)'"
        ),
    )

    load = BashOperator(
        task_id="load_to_warehouse",
        bash_command=(
            f"cat {WORKDIR}/transformed_orders_{{{{ ds_nodash }}}}.json && "
            f"echo 'Đã \"nạp\" dữ liệu vào kho (giả lập) cho ngày {{{{ ds }}}}'"
        ),
    )

    notify = BashOperator(
        task_id="notify_completion",
        bash_command="echo 'Thông báo: pipeline đơn hàng đã hoàn tất thành công!'",
    )

    extract >> transform >> load >> notify
