FROM python:3.12-slim

RUN useradd -u 10001 -m app
WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/main.py .
COPY entrypoint.sh /entrypoint.sh

USER 10001
EXPOSE 8000
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "main.py"]
