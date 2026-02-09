# Step 1: Use an official OpenJDK base image, as Spark requires Java
FROM openjdk:11-jre-slim

# Step 2: Set environment variables for Spark and Python
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Step 3: Install Python, pip, and other necessary tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# Step 4: Download and install Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz && \
    tar -xvf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# Step 5: Set up the application directory
WORKDIR /app

# Step 6: Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Step 7: Copy your application source code
COPY src ./src
COPY config.json .
COPY pyspark_job.py .

# Step 8: Define the entry point for running the PySpark job
ENTRYPOINT ["spark-submit", "pyspark_job.py"]
