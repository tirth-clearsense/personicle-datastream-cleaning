
FROM ubuntu:latest
COPY ./requirements_dataassimilation.txt /app/requirements_dataassimilation.txt

WORKDIR /app
RUN yes | apt-get update -y
RUN yes | apt-get install python3-pip -y
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements_dataassimilation.txt

COPY . /app

CMD ["gunicorn", "--bind", "0.0.0.0:5006", "datapull_decorapi_working:app"]
