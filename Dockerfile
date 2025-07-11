FROM python:3.10-slim
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/main.py .
COPY app/static ./static
COPY app/.env .  # Updated to copy from app/ directory
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--access-log"]
