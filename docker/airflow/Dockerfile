
FROM apache/airflow:2.10.2

USER root

# Configure APT sources and install system dependencies
RUN echo "deb http://deb.debian.org/debian bookworm main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list && \
    rm -f /etc/apt/sources.list.d/* && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-dev \
    build-essential \
    wget \
    openjdk-17-jre-headless \
    procps \
    locales \
    netcat-openbsd \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set timezone to Europe/Paris
RUN echo "Europe/Paris" > /etc/timezone && \
    dpkg-reconfigure -f noninteractive tzdata

# Generate locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3.3.4
ENV SPARK_HOME=/opt/spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download PostgreSQL and Hadoop AWS JARs
ENV SPARK_JARS_DIR=/opt/spark/jars
RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -P ${SPARK_JARS_DIR}/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_JARS_DIR}/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P ${SPARK_JARS_DIR}/

# Clean up /home/airflow/.local and set permissions
RUN rm -rf /home/airflow/.local && \
    mkdir -p /home/airflow/.vscode-server && \
    chown -R airflow:0 ${SPARK_HOME} ${SPARK_JARS_DIR} /home/airflow /home/airflow/.vscode-server && \
    chmod -R u+rwx ${SPARK_HOME} ${SPARK_JARS_DIR} /home/airflow /home/airflow/.vscode-server

USER airflow

# Create virtual environment and install Python dependencies
RUN python3 -m venv /home/airflow/.venv && \
    /home/airflow/.venv/bin/pip install --no-cache-dir --upgrade pip==24.2 && \
    /home/airflow/.venv/bin/pip install --no-cache-dir \
    apache-airflow==2.10.2 \
    dbt-postgres==1.9.0 \
    psycopg2-binary==2.9.9 \
    s3fs==2024.9.0 \
    pandas==2.1.4 \
    pyarrow==17.0.0 \
    pyspark==3.5.3 \
    boto3==1.34.162 \
    pytz==2024.2 \
    fsspec==2024.9.0 \
    "protobuf>=5.0,<6.0" && \
    test -f /home/airflow/.venv/bin/activate || (echo "Virtual environment activation script missing" && exit 1)

# Environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PATH=/home/airflow/.venv/bin:/home/airflow/.local/bin:${SPARK_HOME}/bin:$PATH
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/home/airflow/.venv/lib/python3.12/site-packages:/opt/airflow
ENV TZ=Europe/Paris

# Set working directory
WORKDIR /opt/airflow
