FROM python:2.7-alpine
WORKDIR /code
RUN apk add --no-cache gcc musl-dev linux-headers librdkafka-dev build-base
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
#CMD ["python", "consumer.py"]
