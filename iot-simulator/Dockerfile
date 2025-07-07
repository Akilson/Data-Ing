# Use any lightweight JDK image
FROM openjdk:17-slim

# Create app directory
WORKDIR /app

# Copy your fat JAR into the container
COPY target/scala-2.13/iot-storage.jar iot-storage.jar

# Run the JAR
CMD ["java", "-jar", "iot-storage.jar"]
