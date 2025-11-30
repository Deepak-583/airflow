FROM apache/airflow:3.1.3

# Install required Airflow providers
RUN pip install --no-cache-dir apache-airflow-providers-apache-kafka==1.4.0 


# Remove ALL example DAGs that break Airflow 3.x
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags \
 && rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_extra_dags \
 && rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/example_dags \
 && rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags/plugins \
 && rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow/example_dags/plugins/workday \
 && rm -rf /opt/airflow/dags/example_dags \
 && rm -rf /opt/airflow/example_dags
