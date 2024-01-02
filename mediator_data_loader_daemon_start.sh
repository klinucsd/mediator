
#!/bin/bash

# Function to check PostgreSQL status
check_postgres_status() {
    pg_isready -h localhost -p 5432 -U postgres -d postgres
}

# Wait for PostgreSQL to be up and running
until check_postgres_status; do
    echo "PostgreSQL is not yet ready. Waiting..."
    sleep 1
done

# PostgreSQL is up and running, execute the Python script
echo "PostgreSQL is up and running. Running mediator data loader daemon ..."
python3 /home/pgbouncer/src/data_loader/data_loader_daemon.py &