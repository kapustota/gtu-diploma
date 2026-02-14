FROM python:3.11-slim

# Install Java (required for PySpark) + curl for JAR downloads
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY transform/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Download MongoDB Spark Connector and Mongo Java Driver JARs
RUN mkdir -p /app/jars && \
    curl -fSL -o /app/jars/mongo-spark-connector_2.12-10.4.0.jar \
      https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar && \
    curl -fSL -o /app/jars/mongodb-driver-sync-5.2.1.jar \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.2.1/mongodb-driver-sync-5.2.1.jar && \
    curl -fSL -o /app/jars/mongodb-driver-core-5.2.1.jar \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.2.1/mongodb-driver-core-5.2.1.jar && \
    curl -fSL -o /app/jars/bson-5.2.1.jar \
      https://repo1.maven.org/maven2/org/mongodb/bson/5.2.1/bson-5.2.1.jar

ENV PYSPARK_SUBMIT_ARGS="--jars /app/jars/mongo-spark-connector_2.12-10.4.0.jar,/app/jars/mongodb-driver-sync-5.2.1.jar,/app/jars/mongodb-driver-core-5.2.1.jar,/app/jars/bson-5.2.1.jar pyspark-shell"

CMD ["python", "transform/main.py"]
