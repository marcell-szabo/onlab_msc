FROM openjdk:17-alpine

WORKDIR /kafka
COPY ./producer-1.0-SNAPSHOT.jar /kafka
ENV BOOTSTRAP_SERVER=localhost:9092
ENV INPUT_TOPIC1=input1_topic
ENV INPUT_TOPIC2=input2_topic

ENTRYPOINT java -jar producer-1.0-SNAPSHOT.jar $BOOTSTRAP_SERVER $INPUT1_TOPIC $INPUT_TOPIC2