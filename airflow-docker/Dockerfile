FROM apache/airflow:2.8.1-python3.11

USER root

# Install Java 17 (required for Spark) and procps (Spark uses `ps`)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create arch-agnostic symlink so this Dockerfile works on both arm64 (Apple Silicon) and amd64
RUN ln -sfn /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:/home/airflow/.local/bin:$PATH

USER airflow

# Install PySpark + the Python libs spark_consumer.py needs
RUN pip install --no-cache-dir \
        pyspark==3.5.0 \
        pymongo==4.6.1 \
        psycopg2-binary==2.9.9 \
        python-dotenv==1.0.0
