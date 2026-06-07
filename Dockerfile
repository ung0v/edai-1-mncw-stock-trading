FROM apache/airflow:2.10.2

USER airflow

RUN pip install --no-cache-dir \
    "pandas" \
    "pyarrow" \
    "faker" \
    "pyyaml" \
    "matplotlib" \
    "sqlalchemy" \
    "psycopg2-binary" \
    "confluent-kafka" \
    "pyspark==3.5.1" \
    "delta-spark==3.2.0" \
    "sqlglot" \
    "acryl-datahub[airflow]" \
    "acryl-datahub-airflow-plugin"

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && mkdir -p /opt/spark-extra-jars \
    && curl -fsSL -o /opt/spark-extra-jars/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -fsSL -o /opt/spark-extra-jars/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -fsSL -o /opt/spark-extra-jars/antlr4-runtime-4.9.3.jar https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
    && curl -fsSL -o /opt/spark-extra-jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o /opt/spark-extra-jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL -o /opt/spark-extra-jars/wildfly-openssl-1.0.7.Final.jar https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar \
    && chmod 644 /opt/spark-extra-jars/*.jar \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
