"""
Module 0 - DAG mẫu #1: "Classic style" với Operator truyền thống.

Mục tiêu: làm quen với cú pháp khai báo DAG cơ bản nhất — dùng đối tượng
Operator tường minh (không phải TaskFlow decorator). Đây vẫn là phong cách
rất phổ biến trong các pipeline thực tế, đặc biệt khi dùng các Operator có
sẵn từ provider package (ví dụ SparkSubmitOperator, S3ToRedshiftOperator...)
mà không có logic Python tuỳ biến nào để viết thành hàm.

LƯU Ý QUAN TRỌNG VỀ AIRFLOW 3:
- DAG được import từ `airflow.sdk`, KHÔNG phải `airflow` như Airflow 2.
- BashOperator/PythonOperator không còn nằm trong airflow-core, mà nằm
  trong package riêng `apache-airflow-providers-standard` (đã cài sẵn theo
  mặc định trong image apache/airflow chính thức).
- Dùng tham số `schedule=`, KHÔNG còn `schedule_interval=` như Airflow 2.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def say_hello(**context) -> None:
    """Một hàm Python thuần, được PythonOperator gọi lại lúc task chạy.

    `**context` cho phép ta lấy các biến runtime của Airflow (logical_date,
    ds, task_instance, ...) nếu cần. Ở đây ta chỉ minh hoạ cách đọc `ds`
    (logical_date dạng chuỗi YYYY-MM-DD).
    """
    logical_date_str = context["ds"]
    print(f"Hello từ PythonOperator! DAG này đang xử lý dữ liệu của ngày: {logical_date_str}")


with DAG(
    dag_id="module00_hello_world_classic",
    description="DAG mẫu đầu tiên - phong cách Operator truyền thống (classic style)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["module-00", "hello-world", "classic-style"],
    default_args={
        "owner": "airflow-mastery",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    },
) as dag:

    task_print_date = BashOperator(
        task_id="print_current_date",
        bash_command="date",
    )

    task_say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    task_print_done = BashOperator(
        task_id="print_done",
        bash_command="echo 'Pipeline hello-world classic đã hoàn tất!'",
    )

    # Toán tử >> khai báo thứ tự phụ thuộc: print_date chạy trước, rồi tới
    # say_hello, cuối cùng là print_done. Đây chính là cách bạn "vẽ" DAG
    # (đồ thị có hướng) bằng code.
    task_print_date >> task_say_hello >> task_print_done
