FROM python:3.9-slim-buster

RUN apt-get update && apt-get install -y build-essential
COPY Makefile setup.py README.md ./
RUN mkdir requirements
COPY requirements/base.txt requirements/spark.txt ./requirements/

RUN make bootstrap

COPY . .

RUN pip install .
