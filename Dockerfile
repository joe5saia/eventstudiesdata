FROM python:3.7.4

LABEL maintainer="Joe Saia <joe5saia@gmail.com>"

WORKDIR /app
RUN mkdir /app/data && mkdir /app/output && chmod -R 777 /app
COPY requirements.txt /app/
RUN pip --disable-pip-version-check --no-cache-dir install -r requirements.txt

COPY ./code/ /app
COPY run_python.sh /app

CMD ["bash", "/app/run_python.sh"]
