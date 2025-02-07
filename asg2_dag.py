from airflow import DAG # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime
import os
from google_drive_downloader import GoogleDriveDownloader as gdd  # type: ignore

# Hàm kiểm tra file tồn tại
def check_files():
    file_path = "/opt/airflow/dags/data" 

    # Kiểm tra sự tồn tại của cả hai file
    questions_file = os.path.join(file_path, "Questions.csv")
    answers_file = os.path.join(file_path, "Answers.csv")
    print(f"Checking: {questions_file} -> Exists: {os.path.exists(questions_file)}")
    print(f"Checking: {answers_file} -> Exists: {os.path.exists(answers_file)}")
    if os.path.exists(questions_file) and os.path.exists(answers_file): 
        return 'end' 
    else:
        return 'clear_file'  


def download_file(file_id, dest_path):
    try:
        # Thực hiện tải file từ Google Drive
        gdd.download_file_from_google_drive(file_id=file_id, dest_path=dest_path, unzip=False)

        # Kiểm tra nếu file đã được tải xuống
        if os.path.exists(dest_path):
            print(f"File đã được tải xuống thành công: {dest_path}")
        else:
            print(f"File tải xuống không thành công, không tìm thấy file tại: {dest_path}")
            raise FileNotFoundError(f"Không tìm thấy file tại {dest_path}")
    except Exception as e:
        print(f"Đã xảy ra lỗi khi tải file: {e}")
        raise

with DAG(
    dag_id="Asg2_dag",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False
) as dag:

    # Task bắt đầu và kết thúc
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    # Task kiểm tra file
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=check_files,
        dag=dag
    )

    # Task xóa file cũ
    clear_file = BashOperator(
        task_id='clear_file',
        bash_command='rm -r /opt/airflow/dags/data/*.csv',
        dag=dag
    )

    # Task tải file Questions
    download_question_file_task = PythonOperator(
        task_id='download_question_file_task',
        python_callable=download_file,
        op_kwargs={'file_id': '16nFXawBhTvD0lohkBKICawhBrQwa9KW_', 'dest_path': '/opt/airflow/dags/data/Questions.csv'},
        dag=dag
    )

    # Task tải file Answers
    download_answer_file_task = PythonOperator(
        task_id='download_answer_file_task',
        python_callable=download_file,
        op_kwargs={'file_id': '1ftB370KnU27LWOzXq0-aba-Deai2cVq2', 'dest_path': '/opt/airflow/dags/data/Answers.csv'},
        dag=dag
    )

    uri = 'mongodb://root:rootpassword@mongodb_container:27017/?authSource=admin'
    # Task import dữ liệu Questions vào MongoDB
    import_questions_mongo = BashOperator(
        task_id='import_questions_mongo',
        bash_command=f"mongoimport --uri '{uri}' --type csv -d Asg2 -c Questions --headerline --drop --batchSize 1000 /opt/airflow/dags/data/Questions.csv",
        dag=dag
    )

    # Task import dữ liệu Answers vào MongoDB
    import_answers_mongo = BashOperator(
        task_id='import_answers_mongo',
        bash_command=f"mongoimport --uri '{uri}' --type csv -d Asg2 -c Answers --headerline --drop /opt/airflow/dags/data/Answers.csv",
        dag=dag
    )

    # Task xử lý dữ liệu bằng Spark
    spark_process = SparkSubmitOperator(
        task_id='spark_process',
        application='/opt/airflow/dags/process_data.py',
        conn_id='spark_default',
        packages='org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
        dag=dag
    )

    # Task import kết quả Spark vào MongoDB
    import_output_mongo = BashOperator(
        task_id='import_output_mongo',
        bash_command=f"mongoimport --uri '{uri}' --type csv -d Asg2 -c output --headerline --drop /opt/airflow/dags/output/answers_count.csv",
        dag=dag
    )
    
    # Định nghĩa luồng DAG
    start >> branching
    branching >> end
    branching >> clear_file >> [download_question_file_task, download_answer_file_task]

    download_question_file_task >> import_questions_mongo

    download_answer_file_task >> import_answers_mongo

    [import_questions_mongo, import_answers_mongo] >> spark_process >> import_output_mongo >> end