# sensor-data-streaming-pipeline

Steps to run the pipeline.

- Start Kafka and cassandra using docker-compose in docker folder.
  - cd docker
  - docker-compose up -d
- connect to cassandra container and run sqls in cassandra folder to create namespace and tables
- run spark_job.bat in docker folder to start spark job.
- start spark_data_generator.py to generate sample data in kafka topic.