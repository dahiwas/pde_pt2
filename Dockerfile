# Use Python 3.11 as base and add OpenJDK
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including build tools for psutil
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    curl \
    wget \
    gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME automatically
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy ClickHouse JDBC driver (local file - much faster!)
COPY clickhouse-jdbc-0.3.2-patch9-all.jar /app/clickhouse-jdbc.jar

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories for data processing
RUN mkdir -p /app/uploads /app/temp /app/logs

# Set environment variables for large file handling
ENV PYTHONUNBUFFERED=1
ENV MAX_UPLOAD_SIZE=2147483648
ENV UPLOAD_TIMEOUT=3600

# Expose port
EXPOSE 8000

# Run the application with optimized settings for large files
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "300", "--limit-max-requests", "1000"] 