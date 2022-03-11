FROM python:3.9-buster

EXPOSE 8085

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

RUN apt-get update

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"

COPY . /app

WORKDIR /app/src/main/python

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8085", "--log-level", "debug" ]
