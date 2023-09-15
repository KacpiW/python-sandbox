# README

## Introduction
This application is a Docker-based ETL (Extract, Transform, Load) system. It consists of two main services: `etl_service` and `db`. The `etl_service` is a Python-based ETL service, and `db` is a MySQL database service.

## Prerequisites
Before you can run this application, you need to have Docker and Docker Compose installed on your machine. If you don't have them, you can download them from the official Docker website.

## Setup

1. Clone the repository and navigate to the project's root directory.

2. Set up the required environment variables. This application requires access to your AWS resources, MySQL database credentials and ETL service specific variables. It expects the following environment variables:

   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - AWS_SESSION_TOKEN
   - MYSQL_ROOT_PASSWORD
   - MYSQL_USER
   - MYSQL_PASSWORD
   - MYSQL_HOST="db" (Set docker service name)
   - MYSQL_USER="root"
   - MYSQL_PASSWD="secret"
   - MYSQL_DB="db"
   - EXTRACT_S3_BUCKET (Name of s3 bucket where raw files are located)
   - LOAD_S3_BUCKET (Name of s3 data lake where we want to store our data)
   - LAST_PROCESSED_FILE_NAME (Name of file placed on s3 where, we're going to store last processed file)

   The AWS and MySQL environment variables can be set in your session or in a `.env` file located at the project root. The ETL service specific variables should be added to the `./src/end-to-end/.env` file.

## Notes

This is a basic setup and may need to be adjusted based on your requirements. Particularly, you may want to handle secrets and credentials in a more secure manner for a production environment.
As well as creating dedicated users to connect to production instance of database.

## Running the Application
To run the application, use Docker Compose:

```bash
docker-compose up
```

This command will start both the ETL service and the MySQL service.

The ETL service runs on port 8080 and the MySQL service on port 3306. The ETL service is also configured to be dependent on the MySQL service, so Docker Compose will ensure that the MySQL service is running before starting the ETL service.

## Volumes
The application uses Docker volumes for data persistence and for the initialization of the MySQL service:

- `db_data` is used to store the MySQL data so it remains persistent across runs.
- The current directory is mounted to `/app` in the ETL service container for ease of development.
- `./src/end-to-end/.env` is used for ETL service environment variables.

## Troubleshooting
If you encounter problems running the application, the first step is to check the logs of the individual services:

```bash
docker-compose logs etl_service
docker-compose logs db
```

These commands will display the logs of the ETL service and the MySQL service, respectively.

Please ensure to replace the AWS credentials and other secret information with your own before running the application.
