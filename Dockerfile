FROM apache/airflow:3.2.2

COPY Requirements.txt /Requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.2.2" \
    -r /Requirements.txt