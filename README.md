# Project_Airflow

## Project Requirements
### 1. Task: `start` and `end`
You need to create two tasks, start and end, using DummyOperator to represent the beginning and the end of the DAG.

### 2. Task: `branching`
You need to create a task to check whether the files Questions.csv and Answers.csv have been downloaded and are ready for import.
If the files have not been downloaded, the pipeline proceeds to data processing (moves to the clear_file task).
If the files have been downloaded, the pipeline ends (moves to the end task).
To implement this task, you should use BranchPythonOperator.

### 3. Task: `clear_file`
This will be the first task in the data processing workflow. Before downloading the Questions.csv and Answers.csv files, you need to delete any existing versions of these files to avoid overwrite-related errors. You can accomplish this using BashOperator.

### 4. Task: `download_question_file_task` and `download_answer_file_task`
You need to create two tasks to download the required CSV files.
First, upload the CSV files to your Google Drive.
Then, use the google_drive_downloader library along with PythonOperator to download these files.

### 5. Task: `import_questions_mongo` and `import_answers_mongo`
Once the CSV files are downloaded, you need to import their data into MongoDB for storage. You can use BashOperator with the following mongoimport command:
mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>

### 6. Task: `spark_process`
You need to create a task to use Apache Spark to process the imported data. The processing should include:
Calculating the number of answers for each question.
Using DataFrameWriter to save the processed data as a .csv file.
To complete this task, you can use SparkSubmitOperator to submit a Spark job.

### 7. Task: `import_output_mongo`
After processing the data, store the results in MongoDB using the exported .csv file from Spark. Like the previous import tasks, you can use BashOperator with the mongoimport command.

### 8. Task sequencing and parallel execution
You need to arrange the tasks in the correct order and configure them for parallel execution according to the provided workflow diagram.
