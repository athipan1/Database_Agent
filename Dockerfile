# Stage 1: The Builder
# This stage installs all dependencies into a virtual environment.
FROM python:3.12-slim AS builder

# Prevent Python from writing .pyc files.
ENV PYTHONDONTWRITEBYTECODE 1

# Keep the container logs unbuffered.
ENV PYTHONUNBUFFERED 1

# Create a virtual environment
RUN python -m venv /opt/venv

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Stage 2: The Final Image
# This stage creates the final, lean production image.
FROM python:3.12-slim

# Install curl for the healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the application
RUN addgroup --system app && adduser --system --group app
USER app

# Set working directory
WORKDIR /home/app/code

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Copy the application source code
COPY --chown=app:app . .

# Expose the port the app runs on
EXPOSE 8003

# Add a healthcheck
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:8003/health || exit 1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
