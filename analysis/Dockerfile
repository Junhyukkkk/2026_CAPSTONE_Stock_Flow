FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary 는 휠 사용 → 빌드 의존성 불필요
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
RUN mkdir -p /app/models

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
