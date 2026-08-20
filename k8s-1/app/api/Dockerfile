FROM python:3.12-slim

RUN useradd -u 10001 -m app
WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/main.py .
COPY entrypoint.sh /entrypoint.sh

USER 10001
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
