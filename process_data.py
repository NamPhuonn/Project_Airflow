from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import shutil
import os

def main():
    uri = 'mongodb://root:rootpassword@mongodb_container:27017/?authSource=admin'
    spark = SparkSession \
        .builder \
        .appName("Assignment2") \
        .master("local") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', uri) \
        .config('spark.mongodb.output.uri', uri) \
        .getOrCreate()

    answers_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('database', 'Asg2') \
        .option('collection', 'Answers') \
        .load()

    answers_df = answers_df.withColumn(
        "CreationDate", 
        F.to_date(F.col("CreationDate"), "yyyy-MM-dd HH:mm:ss") 
    )

    answers_df = answers_df.withColumn(
        "OwnerUserId", 
        F.when(F.col("OwnerUserId") == "NA", None).otherwise(F.col("OwnerUserId").cast(IntegerType()))
    )

    questions_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('database', 'Asg2') \
        .option('collection', 'Questions') \
        .load()

    questions_df = questions_df.withColumn(
        "ClosedDate", 
        F.to_date(F.col("ClosedDate"), "yyyy-MM-dd HH:mm:ss") 
    )

    questions_df = questions_df.withColumn(
        "CreationDate", 
        F.to_date(F.col("CreationDate"), "yyyy-MM-dd HH:mm:ss") 
    )

    questions_df = questions_df.withColumn(
        "OwnerUserId", 
        F.when(F.col("OwnerUserId") == "NA", None).otherwise(F.col("OwnerUserId").cast(IntegerType()))
    )

    questions_df = questions_df.withColumnRenamed("Id", "QuestionId") 
    answers_df = answers_df.withColumnRenamed("Id", "AnswerId")

    # Sử dụng Bucket Join (chia bucket cho Questions và Answers)
    questions_df = questions_df.repartitionByRange(4, "QuestionId")
    answers_df = answers_df.repartitionByRange(4, "ParentId")

    # Join Questions với Answers
    joined_df = questions_df.join(
        answers_df,
        questions_df.QuestionId == answers_df.ParentId,
        "inner"
    )   

    output_path = "/opt/airflow/dags/output" 
    # Đếm số lượng câu trả lời cho mỗi câu hỏi
    answers_count = joined_df.groupBy("QuestionId").agg(F.count("AnswerId").alias("Number of Answers"))
    answers_count = answers_count.orderBy("QuestionId")
    answers_count.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    spark.stop()


    # Đổi tên file sau khi lưu
    for filename in os.listdir(output_path):
        if filename.startswith('part-'):
            # Di chuyển file part-00000... vào file answers_count.csv
            shutil.move(os.path.join(output_path, filename), os.path.join(output_path, 'answers_count.csv'))
            break
if __name__ == "__main__":
    main()