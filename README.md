# Data Pipeline Project
This project demonstrates a simple yet scalable data pipeline using PySpark, PostgreSQL, and Apache Airflow for orchestration, all containerized with Docker for easy setup and deployment.

## Project Overview
The pipeline extracts movie and user rating data from a PostgreSQL database, calculates average movie ratings, and writes the results back to the database. The ETL pipeline is implemented using PySpark for efficient large-scale data processing.

Apache Airflow is used to orchestrate and schedule the ETL pipeline, enabling automated and repeatable data workflows.

Docker Compose is included to simplify the setup of Airflow and related services, ensuring a consistent environment across machines.

## Features
- Extract data from PostgreSQL using Spark’s JDBC connector
- Transform data by computing average movie ratings
- Load the transformed data back into PostgreSQL
- Schedule and orchestrate the pipeline using Airflow
- Containerized with Docker Compose for quick environment setup
- Secure management of database credentials using .env file

## Getting Started
### Prerequisites
- Python 3.8+
- Java 8+ (required by Spark)
- Docker & Docker Compose installed
- PostgreSQL database with tables movies and users

### Setup
1. Clone this repository:
```
git clone https://github.com/mginham/data-pipeline-project.git
cd data-pipeline-project
```
2. Configure your environment variables in .env
```
DB_USER=<your_db_username>
DB_PASSWORD=<your_db_password>
DB_URL=jdbc:postgresql://localhost:5432/etl_pipeline
```
3. Start Airflow and dependencies with Docker Compose:
```
docker compose up -d
```
4. Initialize Airflow database and start the webserver (if not already done):
```
docker compose run airflow-init
docker compose up -d
```
5. Access Airflow UI at: http://localhost:8080

### Running the Spark ETL Pipeline
You can run the ETL job standalone:
```
python Exercise\ Files\4-spark_etl_avg_movies_ratings_pipeline.py
```
Or schedule it through Airflow DAGs.

## Project Structure
```
data-pipeline-project/
│
├── airflow/
│   ├── dags/                  # Airflow DAG definitions
│   ├── scripts/               # ETL scripts including 4-spark_etl_avg_movies_ratings_pipeline.py
│   ├── docker-compose.yaml    # Docker Compose config for Airflow
│   └── .env                   # Environment variables for Airflow
│
├── jars/
│   └── postgresql-42.7.7.jar  # PostgreSQL JDBC driver for Spark
│
└── README.md                  # This documentation file
```

## Notes
The Spark ETL uses the PostgreSQL JDBC driver for data connectivity. Ensure the driver .jar file path is correctly set in the Spark session builder.

Airflow currently works best via Docker on Windows due to native compatibility limitations.

Database permissions must allow read/write access for the user specified in .env.

## Future Improvements
- Add automated testing for DAGs and ETL logic
- Integrate cloud storage or data warehouse connectors
- Implement monitoring and alerting for Airflow pipelines
- Parameterize ETL jobs for flexibility and scalability

## License
This project is licensed under the MIT License.