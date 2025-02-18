
# Invest Lakehouse Py Spark Project

This project is an application to extract, load and transform data from postgres to lakehouse using py spark on bronze and silver layers.

## Getting Started

To start the project, follow the commands below:

### Docker Commands

- **Start server:**
  ```bash
  docker-compose up -d 
  ```

- **Enter on server:**
  ```bash
  docker exec -it pyspark-container bash
  ```

- **Run ingestion to bronze layer:**
  ```bash
  python3 app/ingestion/bronze/main.py
  ```

  - **Run transformation from bronze layer to silver layer:**
  ```bash
  python3 app/transform/silver/main.py
  ```

