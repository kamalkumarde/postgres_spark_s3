#!/bin/bash
set -e

echo "Starting entrypoint.sh as $(whoami)"

# Ensure permissions for /home/airflow/.vscode-server
if [ -d "/home/airflow/.vscode-server" ]; then
    echo "Fixing permissions for /home/airflow/.vscode-server"
    chown -R airflow:0 /home/airflow/.vscode-server 2>/dev/null || {
        echo "Warning: Failed to set permissions for /home/airflow/.vscode-server. Continuing anyway."
    }
    chmod -R u+rwx /home/airflow/.vscode-server 2>/dev/null || {
        echo "Warning: Failed to set permissions for /home/airflow/.vscode-server. Continuing anyway."
    }
else
    echo "Creating /home/airflow/.vscode-server"
    mkdir -p /home/airflow/.vscode-server
    chown airflow:0 /home/airflow/.vscode-server
    chmod u+rwx /home/airflow/.vscode-server
fi

# Check for virtual environment
if [ ! -f "/home/airflow/.venv/bin/activate" ]; then
    echo "Error: Virtual environment activation script /home/airflow/.venv/bin/activate not found"
    exit 1
fi

# Wait for airflow-postgres to be ready
echo "Waiting for airflow-postgres to be ready"
until nc -z airflow-postgres 5432; do
    echo "airflow-postgres:5432 - no response"
    echo "airflow-postgres is not ready, retrying in 5 seconds..."
    sleep 5
done
echo "airflow-postgres:5432 - accepting connections"

# Execute the command passed to the entrypoint
echo "Executing command: $@"
exec "$@"
