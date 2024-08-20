# Batch and Micro-batch Processing with Pandas, Spark, and Spark Streaming

This repository contains scripts for performing batch processing with Pandas and Apache Spark, as well as micro-batch processing using Spark Streaming. The repository also includes a `docker-compose.yml` file to set up a executing environment with distributed computing implementation, also allowing to output the graph interactively on Jupyter-Notebook environment on Docker.

## Contents

- **docker-compose.yml**: A Docker Compose configuration file that sets up a executing environment for 3 python files below, containing Spark cluster with one master and three worker nodes, along with additional services like Kafka, and Zookeeper to support streaming and interactive analysis.

- **Batch_Processing_with_Pandas.py**: A Python script that demonstrates batch processing using the Pandas library. This script reads datasets, performs batch processing, and outputs the processed data.
  
- **Batch_Processing_with_Spark.py**: A Python script for batch processing using Apache Spark. This script utilise Kafka for storing data before processing them. And instead of using Pandas DataFrame, this code uses Spark DataFrame for processing data.
  
- **Micro-batch_Processing_with_Spark_Streaming.py**: This script demonstrates micro-batch processing using Spark Streaming. It processes streaming data in small, continuous batches, reading and processing data every second with 3 Spark Worker nodes distribution.

## Prerequisites

Before running the scripts, ensure you have the following installed:

- Python 3.x
- Apache Spark
- Docker 4.3x

## Usage

### Using Docker Compose

1. **Start the Docker Environment**:
   - Navigate to the directory containing the `docker-compose.yml` file.
   - Run the following command to set up the environment:
     ```bash
     docker-compose up
     ```

2. **Accessing Jupyter Notebook**:
   - After the services are up and running, access the Jupyter Notebook by navigating to `http://localhost:8888` in your web browser. No token is required to access the notebook.

3. **Running Python Files**:
   - You can access to Jupyter container and paste Python files to execute batch processing and micro-batch processing.

4. **Kafka and Zookeeper**:
   - Kafka and Zookeeper are included for handling streaming data. Kafka topics can be managed via connecting to Kafka container on terminal with the the following command:
     ```bash
     docker exec -it <kafka_container_id> /bin/bash
     ```

### Running the Batch Processing with Pandas Scripts

**Pandas DataFrame**:
   - Access to Jupyter container and paste the `Batch_Processing_with_Pandas.py`, and run the code.

### Running the Batch Processing with Spark Scripts

**Spark DataFrame**:
   - Ensure Spark is installed (installed by default) and properly configured.
   - Access to Jupyter container and paste the `Batch_Processing_with_Spark.py`, and run the code.

### Running the Micro-batch Processing with Spark Streaming Script

**Spark Streaming**:
   - Ensure Spark is installed (installed by default) and properly configured.
   - Access to Jupyter container and paste the `Micro-batch_Processing_with_Spark_Streaming.py`, and run the code.

## Contributing

Contributions are welcome! Please fork this repository, create a new branch, and submit a pull request.

## Contact

For questions or further assistance, please contact the repository maintainer.
