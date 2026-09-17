Real-Time E-Commerce Data Engineering Platform
📌 Project Overview
This project is an end-to-end real-time e-commerce data engineering platform.
It reads e-commerce transaction data, streams the records through Apache Kafka, processes the stream with Apache Spark, stores processed data in PostgreSQL, orchestrates workflows with Apache Airflow, and provides analytics through Power BI.
The project is designed to demonstrate a practical real-time data engineering pipeline using Docker and modern data engineering technologies.
🎯 Project Goal
The main goal is to build a complete real-time e-commerce data pipeline that demonstrates:
•	Real-time data ingestion
•	Message streaming
•	Real-time data processing
•	Data transformation
•	Data storage
•	Workflow orchestration
•	Data quality checks
•	Data visualization
•	Containerized deployment
📦 Dataset
The project uses the **Online Retail Dataset** from the UCI Machine Learning Repository.
Dataset:
https://archive.ics.uci.edu/dataset/352/online+retail
Dataset Information
•	Records: 541,909
•	Time period: 2010–2011
•	Original file: `Online Retail.xlsx`
•	Format: Excel (`.xlsx`)
•	Domain: E-commerce transactions
The full dataset is used for the main pipeline.
A smaller 10,000-row CSV sample is retained for quick development and testing.
Main Columns
| Column | Description |
|---|---|
| InvoiceNo | Invoice/order number |
| StockCode | Product code |
| Description | Product description |
| Quantity | Quantity purchased |
| InvoiceDate | Transaction date and time |
| UnitPrice | Price per item |
| CustomerID | Customer identifier |
| Country | Customer country |
Dataset Location
```text
data/
├── raw/
│   └── Online Retail.xlsx
└── sample/
    └── ecommerce_sample.csv
```
🧹 Data Preparation
The original Excel dataset is kept unchanged in `data/raw/`.
The project performs data quality checks before processing the data.
Checks include:
•	Missing values
•	Duplicate records
•	Invalid values
•	Data types
•	Required fields
•	Negative quantities
•	Invalid prices
Duplicate records and other invalid records are handled in the working/processing stage without modifying the original raw dataset.
Data Preparation Flow
```text
Online Retail.xlsx
       ↓
Data Quality Checks
       ↓
Clean/Validated Data
       ↓
Python Producer
       ↓
Kafka
```
🐍 Python Producer
The Python producer reads the full e-commerce Excel dataset using Pandas.
Each transaction is converted into a JSON event and sent to Kafka.
Example Event
```json
{
  "invoice_no": "536365",
  "stock_code": "85123A",
  "description": "WHITE HANGING HEART",
  "quantity": 6,
  "invoice_date": "2010-12-01 08:26:00",
  "unit_price": 2.55,
  "customer_id": "17850",
  "country": "United Kingdom"
}
```
Producer Flow
```text
Online Retail.xlsx
       ↓
Pandas
       ↓
Python Producer
       ↓
JSON Event
       ↓
Kafka
```
The producer supports the full dataset of **541,909 records**.
📨 Apache Kafka
Apache Kafka is used as the real-time messaging system.
Kafka runs using Docker.
Kafka Topic
```text
ecommerce_orders
```
Current Kafka Pipeline
```text
Python Producer
       ↓
Kafka
       ↓
ecommerce_orders
```
Kafka producer and consumer testing has been completed.
The producer can send the full dataset to the Kafka topic without an artificial delay.
🏗️ Architecture
```text
                 ┌───────────────┐
                 │ E-Commerce    │
                 │ Dataset       │
                 └───────┬───────┘
                         ↓
                 ┌───────────────┐
                 │ Python        │
                 │ Producer      │
                 └───────┬───────┘
                         ↓
                 ┌───────────────┐
                 │ Apache Kafka  │
                 └───────┬───────┘
                         ↓
                 ┌───────────────┐
                 │ Spark         │
                 │ Streaming     │
                 └───────┬───────┘
                         ↓
                 ┌───────────────┐
                 │ PostgreSQL    │
                 └───────┬───────┘
                         ↓
                 ┌───────────────┐
                 │ Power BI      │
                 └───────────────┘
                 Apache Airflow
                 Pipeline Management
```
🔄 Complete Data Flow
1. The Online Retail Excel dataset is read by Python.
2. Python validates and prepares the records.
3. Python converts each transaction into a JSON event.
4. Python sends events to Kafka.
5. Kafka receives and manages the event stream.
6. Spark Streaming reads events from Kafka.
7. Spark validates and transforms the data.
8. Spark writes processed data to PostgreSQL.
9. Airflow manages and monitors pipeline workflows.
10. Power BI connects to PostgreSQL.
11. Power BI displays e-commerce analytics.
🛠️ Technologies
•	Python
•	Pandas
•	Apache Kafka
•	Apache Spark
•	Spark Structured Streaming
•	PostgreSQL
•	Apache Airflow
•	Docker
•	Power BI
•	Git
•	GitHub
📁 Project Structure
```text
real-time-ecommerce-platform/
├── data/
│   ├── raw/
│   │   └── Online Retail.xlsx
│   └── sample/
│       └── ecommerce_sample.csv
│
├── producer/
│   ├── __init__.py
│   ├── producer.py
│   ├── config.py
│   └── requirements.txt
│
├── kafka/
│   └── README.md
│
├── spark/
│   ├── __init__.py
│   ├── streaming_job.py
│   ├── transformations.py
│   └── config.py
│
├── postgres/
│   ├── init.sql
│   ├── schema.sql
│   └── queries.sql
│
├── airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py
│   ├── plugins/
│   └── requirements.txt
│
├── dashboard/
│   ├── ecommerce_dashboard.pbix
│   └── screenshots/
│
├── tests/
│   ├── test_producer.py
│   ├── test_transformations.py
│   └── test_database.py
│
├── docs/
│   ├── architecture.png
│   ├── data_flow.png
│   └── screenshots/
│
├── logs/
│   └── .gitkeep
│
├── README.md
├── .gitignore
├── docker-compose.yml
└── requirements.txt
```
⚙️ Prerequisites
Install:
•	Python 3.x
•	Docker Desktop
•	Git
•	Power BI Desktop
Recommended:
•	16 GB RAM
•	SSD storage
•	At least 50 GB free disk space
🚀 Installation
Clone the repository:
```bash
git clone https://github.com/YOUR-USERNAME/real-time-ecommerce-platform.git
```
Go to the project directory:
```bash
cd real-time-ecommerce-platform
```
Install dependencies:
```bash
pip install -r requirements.txt
```
Start Docker services:
```bash
docker compose up -d
```
Check running containers:
```bash
docker ps
```
▶️ How to Run
1. Start Docker services
```bash
docker compose up -d
```
2. Check Kafka
```bash
docker ps
```
3. Check Kafka topic
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```
Expected topic:
```text
ecommerce_orders
```
4. Run Python Producer
```bash
python producer/producer.py
```
The producer reads:
```text
data/raw/Online Retail.xlsx
```
and sends the records to:
```text
ecommerce_orders
```
5. Start Spark Streaming
The Spark streaming job will consume messages from Kafka and process them.
6. Store data in PostgreSQL
Processed records will be loaded into PostgreSQL.
7. Run Airflow
Airflow will manage and monitor the pipeline.
8. Open Power BI
Power BI will connect to PostgreSQL and display the final analytics.
📊 Dashboard
Power BI will be used to visualize e-commerce data.
Planned dashboard metrics:
•	Total Sales
•	Total Orders
•	Total Revenue
•	Total Products
•	Total Customers
•	Average Order Value
•	Sales Trends
•	Sales by Country
•	Top Products
•	Real-Time Activity
🗄️ Database
PostgreSQL will store processed e-commerce data.
Planned tables include:
•	Customers
•	Products
•	Orders
•	Order Items
•	Sales
The database design will be finalized during the Spark and PostgreSQL stages.
🔁 Apache Airflow
Apache Airflow will be used for workflow orchestration.
Airflow will:
•	Schedule workflows
•	Manage pipeline tasks
•	Manage task dependencies
•	Monitor task execution
•	Provide task logs
•	Handle pipeline failures
🧪 Testing
Testing will cover:
•	Dataset validation
•	Python Producer
•	Kafka messages
•	Spark transformations
•	PostgreSQL loading
•	Airflow workflows
Tests will be added as each pipeline component is completed.
🔐 Data Quality
The pipeline will include basic data quality checks:
•	Missing value checks
•	Duplicate checks
•	Data type validation
•	Required field validation
•	Invalid quantity checks
•	Invalid price checks
The original raw dataset will remain unchanged.
🐳 Docker
Docker is used to run the project services in containers.
Current services:
•	Kafka
•	Zookeeper
Planned services:
•	Spark
•	PostgreSQL
•	Airflow
📈 Current Project Progress
Completed
•	Project structure
•	Git/GitHub setup
•	Dataset downloaded
•	Dataset inspection
•	10,000-row sample created
•	Data quality checks started
•	Python producer created
•	Kafka configured
•	Kafka topic created
•	Kafka producer/consumer tested
•	Full 541,909-record dataset connected to the producer
In Progress
•	Full Kafka ingestion
•	Spark Streaming
•	Data transformations
•	PostgreSQL
•	Airflow
•	Power BI dashboard
•	Automated testing
📸 Screenshots
Screenshots will be added during project completion.
Planned screenshots:
•	Project structure
•	Kafka containers
•	Kafka topic
•	Producer output
•	Spark Streaming
•	PostgreSQL
•	Airflow
•	Power BI dashboard
📈 Project Results
Final results will include:
•	Number of records processed
•	Kafka ingestion results
•	Spark processing results
•	PostgreSQL loading results
•	Data quality results
•	Dashboard results
•	Pipeline execution results
⚠️ Challenges
Potential challenges include:
•	Processing 541,909 records
•	Kafka configuration
•	Spark Streaming configuration
•	Database connectivity
•	Docker networking
•	Data quality
•	Pipeline failures
•	Large-volume data ingestion
✅ Solutions
Solutions and important technical decisions will be documented during development.
Examples:
•	Using Kafka for scalable event ingestion
•	Using Spark Structured Streaming for stream processing
•	Using Docker for consistent environments
•	Keeping the raw dataset unchanged
•	Adding data quality checks
•	Testing with a smaller sample before full-scale processing
🚀 Future Improvements
Possible future improvements:
•	AWS deployment
•	Amazon S3
•	Data Lake
•	Data Lakehouse
•	dbt
•	CI/CD
•	Advanced monitoring
•	Cloud deployment
•	Advanced data quality checks
•	Kafka partitioning and scaling
•	Performance optimization
📚 What I Learned
This project demonstrates practical experience with:
•	Python
•	Pandas
•	Kafka
•	Spark Streaming
•	PostgreSQL
•	Airflow
•	Docker
•	Power BI
•	Real-time data pipelines
•	Data transformation
•	Data quality
•	Event-driven architecture
•	Data engineering architecture
👨‍💻 Author
**Your Name**
GitHub:
https://github.com/YOUR-USERNAME
📄 License
This project is created for educational and portfolio purposes.
