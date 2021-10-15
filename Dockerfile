FROM prefecthq/prefect:latest
ENV PYTHONPATH /app
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
RUN chown -R 1000:1000 /app
USER 1000
COPY flow_utilities .
