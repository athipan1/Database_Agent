# Stage 1: The Builder
# This stage builds a virtual environment with all dependencies.
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Create and activate a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install production dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---
# Stage 2: The Final Production Image
# This stage creates a lean image with only what's needed to run the application.
FROM python:3.12-slim

# Install curl for the healthcheck and upgrade system packages for security
RUN apt-get update && apt-get upgrade -y && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN addgroup --system app && adduser --system --group app
USER app

# Set a working directory
WORKDIR /home/app/code

# Copy the virtual environment with only production dependencies from the builder
COPY --from=builder /opt/venv /opt/venv

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Copy the application source code
COPY --chown=app:app . .

# Expose the port the app will run on
EXPOSE 8000

# Add a healthcheck to ensure the application is responsive
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# The command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
