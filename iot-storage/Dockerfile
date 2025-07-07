FROM hseeberger/scala-sbt:11.0.11_1.5.5_2.13.6
WORKDIR /app

# Install curl only, no Java install needed
RUN apt-get update && apt-get install -y curl wget

# Install Spark 3.5.0 manually
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar zx -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Hadoop AWS dependencies for S3 / MinIO support
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

# Copy code and build
COPY . .
RUN sbt clean compile

CMD ["sbt", "run"]
