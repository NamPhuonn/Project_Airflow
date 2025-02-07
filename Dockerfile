FROM apache/airflow:2.5.1-python3.10

# Cập nhật apt và cài đặt git
USER root
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir /content
# mongosh
RUN wget -qO "/content/mongodb-mongosh_amd64.deb" "https://downloads.mongodb.com/compass/mongodb-mongosh_1.9.1_amd64.deb"
RUN dpkg -i "/content/mongodb-mongosh_amd64.deb"

# mongodb-database-tools
RUN wget -qO "/content/mongodb-database-tools.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian11-x86_64-100.7.1.deb"
RUN dpkg -i "/content/mongodb-database-tools.deb"

# Chuyển sang người dùng airflow để cài đặt pip và các thư viện
USER airflow
RUN pip install --user --upgrade pip

# Cài đặt thêm các module qua pip
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    apache-airflow-providers-apache-spark==4.0.0 \
    pyspark==3.2.1 \
    findspark==1.4.2
