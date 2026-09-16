FROM apache/spark:3.5.6-python3

USER root

RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    pyarrow==14.0.2 \
    boto3==1.35.81

# ------------------------------------------------------------
# Hadoop AWS
# ------------------------------------------------------------

RUN curl -L \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/project

COPY requirements.txt /opt/project/requirements.txt

RUN pip install --no-cache-dir \
    -r /opt/project/requirements.txt

USER spark