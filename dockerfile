FROM python:3.7.4

LABEL maintainer="Joe Saia <joe5saia@gmail.com>"

WORKDIR /app
RUN mkdir /app/data
COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY ./code/ /app

CMD ["python3", "data_grabs.py"]
