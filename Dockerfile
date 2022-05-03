FROM python:3.10-buster

RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common

EXPOSE 8085

COPY . /app
RUN pip install -r /app/requirements.txt

WORKDIR /app/src/main/python

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8085", "--log-level", "debug" ]
