FROM flink:2.0.0-scala_2.12

# Install Python e pip
USER root

# Update and install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget openjdk-17-jdk openjdk-17-source && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Symlink for pemja
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /opt/java/openjdk
RUN ln -s /usr/lib/jvm/java-17-openjdk-*/include /opt/java/openjdk/include

# Install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Download Kafka jars
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar

USER flink

RUN python3 --version && pip3 --version
