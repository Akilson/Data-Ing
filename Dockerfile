FROM hseeberger/scala-sbt:graalvm-ce-21.3.0-java17_1.6.2_3.1.1
WORKDIR /app

# Copy the entire multi-project context needed for build
COPY build.sbt .
COPY project/ project/
COPY iot-simulator/ iot-simulator/

# Build all projects
RUN sbt clean compile

# Default command runs your iotSimulator subproject
CMD ["sbt", "iotSimulator/run"]