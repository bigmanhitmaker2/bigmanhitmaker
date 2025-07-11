FROM python:3.10-slim

# Install redis-server
RUN apt-get update && apt-get install -y redis-server

# Set the working directory
WORKDIR /app

# Copy files into the container
COPY app/main.py .
COPY app/requirements.txt .
COPY app/static ./static
COPY .env .

# Install dependencies
RUN pip install -r requirements.txt

# Expose the application port
EXPOSE 8000

# Start Redis and Uvicorn
CMD redis-server --daemonize yes && uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info --access-log

