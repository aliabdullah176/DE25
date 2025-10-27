FROM python:3.9

RUN pip install numpy
RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "ingest_data.py" ]
