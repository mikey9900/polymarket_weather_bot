FROM python:3.11-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
COPY run.sh /run.sh
RUN chmod +x /run.sh
CMD ["/run.sh"]
