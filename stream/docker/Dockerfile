FROM openjdk:17-alpine

WORKDIR /kafka
COPY ./onlab1_msc-1.0-SNAPSHOT.jar /kafka
ENV BOOTSTRAP_SERVER=localhost:9092
ENV INPUT_TOPIC=input_topic
ENV OUTPUT_TOPIC=output_topic

ENTRYPOINT java -jar onlab1_msc-1.0-SNAPSHOT.jar $BOOTSTRAP_SERVER $INPUT_TOPIC $OUTPUT_TOPIC