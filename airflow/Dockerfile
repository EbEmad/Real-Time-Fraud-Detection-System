FROM apache/airflow:2.10.5 as airflow

# Create models directory and set ownership
USER root
# Copy the entrypoint script
COPY  scripts/Airflow.sh /Airflow.sh
RUN chmod +x /Airflow.sh

# Switch back to airflow user for safety
USER airflow
# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages as root (system-wide)
RUN pip install -r /tmp/requirements.txt

# Set the entrypoint to the entrypoint script
ENTRYPOINT ["/Airflow.sh"]