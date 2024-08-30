
FROM python:latest

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow
RUN mkdir -p /home/pipe/logs

WORKDIR /home/pipe

COPY ingest.py ingest.py

ENTRYPOINT [ "python", "ingest.py" ]