# ใช้ official Python image
FROM python:3.12-slim

# กำหนด working directory ใน container
WORKDIR /code

# Copy ไฟล์ requirements.txt และติดตั้ง dependencies
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy โค้ดทั้งหมดในโปรเจกต์เข้าไปใน container
COPY . /code/

# กำหนด Command ที่จะรันเมื่อ container เริ่มทำงาน
# รัน API server ด้วย Uvicorn บน port 8003
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
