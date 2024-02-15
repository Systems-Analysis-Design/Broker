FROM openjdk:21
WORKDIR /app
COPY ./target/broker-1.0-SNAPSHOT.jar /app
EXPOSE 30012
CMD ["java", "-jar", "demo-1.0-SNAPSHOT.jar"]
