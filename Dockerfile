FROM jupyter/minimal-notebook:29f53f8b9927

COPY Makefile requirements.txt setup.py README.md ./

RUN make bootstrap

COPY . .

RUN pip install .
