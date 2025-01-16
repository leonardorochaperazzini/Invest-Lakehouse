FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y python3 python3-pip

COPY /app /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /workdir

CMD ["bash"]
