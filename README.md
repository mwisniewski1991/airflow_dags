# Airflow DAGs

## Description
Repository containing DAG examples demonstrating Airflow functionalities and features.

## DAGs

### postgresql_backup

**Description**:
Verify database modifications and create a backup if changes are detected. The DAG is designed to create a maximum of one backup per day.

**Prerequisites**:
- PostgreSQL database with a dedicated user
- Defined Airflow PostgreSQL connection
- Create temp directory

**Task list:**
- check_last_update
    - Type: Sensor
    - Function: 
        - Establish a connection to the database and verify if updates have occurred
        - The sensor is scheduled to execute hourly for a duration of one day. If no database updates are detected, the sensor will return False, and the DAG will be marked as successful.
        - The database schema incorporates a column that tracks the most recent update for each row.
            - Alternative approach: Enumerate the row count and store this value in a file for comparison with the previous day's data.
        
- create_backup_postgresql
    - Type: Task
    - Function:
        - Generate a backup of the PostgreSQL database
        - Use the subprocess library to execute the pg_dump command
        - Command to execute:
            - pg_dump postgresql://user:password@host:port/database_name > /path_to_backup/file_name.sql

**Note**:
- File with backup is stored in the `temp` directory.
- File name is generated using a CET timestamp of the backup creation.