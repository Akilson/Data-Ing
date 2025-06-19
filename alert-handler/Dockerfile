FROM hseeberger/scala-sbt:11.0.17_1.8.0_2.13.11

WORKDIR /app

# Copy build files
COPY build.sbt .

# Copy source code
COPY src/ src/

# Build the application
RUN sbt clean compile

# Run the application
CMD ["sbt", "run"]
