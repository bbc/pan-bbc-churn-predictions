FROM python:3.7.7

RUN apt-get update
RUN apt-get  install -y python3-pip
RUN apt-get install -y r-base
RUN apt-get install nano
RUN pip3 install awscli --upgrade --ignore-installed six

COPY scripts/run-pan-bbc-churn-predictions.sh  /usr/local/bin/
COPY requirements.txt /usr/local/bin

RUN pip3 install -r /usr/local/bin/requirements.txt
RUN chmod 755 /usr/local/bin/run-pan-bbc-churn-predictions.sh

