FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN apt-get update && apt-get install -y build-essential gdal-bin libgdal-dev libgeos-dev \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apt-get remove -y build-essential libgdal-dev \
    && apt-get autoremove -y

COPY . /app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

CMD ["python", "main.py"]
