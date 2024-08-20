# Batch Processing with Pandas and Spark, and Micro-batch with Spark Streaming

This repository contains scripts for performing batch processing with Pandas and Apache Spark, as well as micro-batch processing using Spark Streaming. The repository also includes a `docker-compose.yml` file to set up a distributed processing environment using Docker.

## Contents

- **Batch_Processing_with_Pandas.py**: A Python script that demonstrates batch processing using the Pandas library. This script reads large datasets, performs data transformations, and outputs the processed data.
  
- **Batch_Processing_with_Spark.py**: A Python script for batch processing using Apache Spark. It leverages Spark's distributed computing capabilities to efficiently process large datasets.
  
- **Micro-batch_Processing_with_Spark_Streaming.py**: This script demonstrates micro-batch processing using Spark Streaming. It processes streaming data in small, continuous batches, enabling near real-time data analysis.
  
- **docker-compose.yml**: A Docker Compose configuration file that sets up a Spark cluster with one master and three worker nodes, along with additional services like Jupyter, Kafka, and Zookeeper to support streaming and interactive analysis.

## Prerequisites

Before running the scripts, ensure you have the following installed:

- Python 3.x
- Apache Spark
- Docker and Docker Compose (for setting up the environment)

## Usage

### Running the Batch Processing Scripts

1. **Pandas Batch Processing**:
   - Run the `Batch_Processing_with_Pandas.py` script using Python:
     ```bash
     python Batch_Processing_with_Pandas.py
     ```

2. **Spark Batch Processing**:
   - Ensure Spark is installed and properly configured.
   - Run the `Batch_Processing_with_Spark.py` script:
     ```bash
     spark-submit Batch_Processing_with_Spark.py
     ```

### Running the Micro-batch Processing Script

1. **Spark Streaming**:
   - Ensure your Spark environment is configured for streaming.
   - Run the `Micro-batch_Processing_with_Spark_Streaming.py` script:
     ```bash
     spark-submit Micro-batch_Processing_with_Spark_Streaming.py
     ```

### Using Docker Compose

1. **Start the Docker Environment**:
   - Navigate to the directory containing the `docker-compose.yml` file.
   - Start the Spark cluster and related services by running:
     ```bash
     docker-compose up
     ```

2. **Accessing Jupyter Notebook**:
   - After the services are up and running, access the Jupyter Notebook by navigating to `http://localhost:8888` in your web browser. No token is required to access the notebook.

3. **Running Spark Jobs**:
   - You can submit Spark jobs either through the command line or directly from the Jupyter notebook.

4. **Kafka and Zookeeper**:
   - Kafka and Zookeeper are included for handling streaming data. Kafka topics can be managed via the `docker-compose.yml` configuration.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please fork this repository, create a new branch, and submit a pull request.

## Contact

For questions or further assistance, please contact the repository maintainer.
