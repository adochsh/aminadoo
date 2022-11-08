FROM python:3.8-slim-buster

LABEL version=1.0

WORKDIR /app

ENV DB_USER="APPROGSR"
ENV DB_PASS="ZkgUnURVdKFwr3eX"
ENV DB_HOST="nxlrd1-scan1.int.adeo.com"
ENV DB_SERV="SLMRUGO1"
ENV ORACLE_HOME=/usr/lib/oracle
ENV TNS_ADMIN=/usr/lib/oracle
ENV LD_LIBRARY_PATH=/usr/lib/oracle/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}


COPY . /app/
COPY ./.instantclient_19_15 /usr/lib/oracle/lib


RUN apt-get clean
RUN apt-get update
RUN apt-get install libaio1
RUN python -m pip install --upgrade pip

RUN pip3 install -r /app/requirements.txt

CMD streamlit run /app/app.py 
