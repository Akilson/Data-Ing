FROM hseeberger/scala-sbt:11.0.16_1.7.1_2.13.8

WORKDIR /app

# Copy build files
COPY build.sbt .
COPY project/ project/

# Copy source code
COPY src/ src/

# Build the application
RUN sbt clean compile

# Run the application
CMD ["sbt", "run"]
