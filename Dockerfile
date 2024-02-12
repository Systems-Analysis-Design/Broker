FROM openjdk:21
WORKDIR /app
COPY ./target/broker-1.0-SNAPSHOT.jar /app
EXPOSE 8080
CMD ["java", "-jar", "demo-1.0-SNAPSHOT.jar"]
