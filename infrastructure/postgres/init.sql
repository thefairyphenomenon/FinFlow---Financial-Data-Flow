-- infrastructure/postgres/init.sql
-- Runs automatically when PostgreSQL container starts for the first time

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create Airflow database
CREATE DATABASE airflow_db;

-- Grant all privileges to app user
GRANT ALL PRIVILEGES ON DATABASE dataflow_db TO dataflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO dataflow_user;
