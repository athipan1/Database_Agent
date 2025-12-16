# -------- Base Image --------
FROM python:3.9-slim

# -------- Environment --------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# -------- Workdir --------
WORKDIR /app

# -------- System deps (optional แต่แนะนำ) --------
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# -------- Python deps --------
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# -------- App source --------
COPY . .

# -------- Default command --------
CMD ["python", "trading_db.py"]