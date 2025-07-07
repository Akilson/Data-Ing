#! /bin/bash

cd iot-simulator
sbt clean assembly
cd ../alert-selector
sbt clean assembly
cd ../alert-handler
sbt clean assembly
cd ../

docker compose up --build
