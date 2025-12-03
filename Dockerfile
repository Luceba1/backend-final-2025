# Use an official Python runtime as a parent image
FROM python:3.11.6-slim-bullseye AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set the working directory in the container to /app
WORKDIR /app

# Install system dependencies required for PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

RUN pip install --no-cache-dir --user -r requirements.txt

# Start a new stage to create a smaller image
FROM python:3.11.6-slim-bullseye

# Set environment variables for production
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/root/.local/bin:$PATH

# Install runtime dependencies (libpq5 for PostgreSQL, curl for health checks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from previous stage
COPY --from=builder /root/.local /root/.local

# Set the working directory in the container to /app
WORKDIR /app

# Create logs directory
RUN mkdir -p /app/logs

# Copy the current directory contents into the container at /app
COPY . .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health_check || exit 1

# Expose port
EXPOSE 8000

# Run main.py when the container launches
CMD ["python", "-m", "main"]