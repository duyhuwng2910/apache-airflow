"""
Module 0 - DAG mẫu #2: TaskFlow API hiện đại (@dag / @task).

Mục tiêu: làm quen với phong cách viết DAG bằng hàm Python thuần + decorator,
là cách được khuyến nghị cho hầu hết logic Python tuỳ biến từ Airflow 2.0
trở đi, và càng được nhấn mạnh ở Airflow 3 (xem Module 3 để đi sâu).

So với DAG "classic" (hello_world_classic_dag.py), điểm khác biệt cốt lõi:
- Không cần gọi PythonOperator(...) thủ công — chỉ cần decorator @task.
- Không cần tự quản lý XCom bằng tay — giá trị return của hàm tự động
  được truyền cho hàm nhận nó làm tham số.
- Thứ tự phụ thuộc được suy ra tự động từ việc bạn gọi hàm nào lấy kết quả
  của hàm nào, thay vì phải viết tường minh bằng >>.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="module00_hello_world_taskflow",
    description="DAG mẫu đầu tiên - phong cách TaskFlow API (@dag/@task)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["module-00", "hello-world", "taskflow"],
)
def hello_world_taskflow():

    @task
    def get_greeting_name() -> str:
        """Task đầu tiên: trả về một cái tên để chào.

        Giá trị return này sẽ tự động được Airflow đẩy qua XCom và truyền
        cho task tiếp theo gọi nó — bạn không cần viết bất kỳ dòng code
        XCom thủ công nào.
        """
        return "Data Engineer"

    @task
    def build_greeting(name: str) -> str:
        """Task thứ hai: nhận tên từ task trước, ghép thành câu chào."""
        greeting = f"Xin chào, {name}! Đây là DAG TaskFlow đầu tiên của bạn."
        print(greeting)
        return greeting

    @task
    def log_final_message(message: str) -> None:
        """Task cuối: chỉ in ra để xác nhận toàn bộ chuỗi đã chạy đúng."""
        print(f"[KẾT THÚC PIPELINE] {message}")

    # Việc gọi các hàm task lồng nhau như dưới đây chính là cách TaskFlow
    # tự suy ra đồ thị phụ thuộc: get_greeting_name -> build_greeting -> log_final_message
    name = get_greeting_name()
    greeting = build_greeting(name)
    log_final_message(greeting)


# Bắt buộc gọi hàm đã decorate để Airflow thực sự khởi tạo đối tượng DAG
hello_world_taskflow()
