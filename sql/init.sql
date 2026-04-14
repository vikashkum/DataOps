-- Create a separate database for Airflow metadata so it doesn't
-- share the dataops warehouse database.
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO dataops;
