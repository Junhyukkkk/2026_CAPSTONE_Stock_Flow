FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary 는 휠 사용 → 빌드 의존성 불필요
# lightgbm 은 OpenMP 런타임(libgomp1)이 필요하다(slim 이미지에는 없음)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
RUN mkdir -p /app/models

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
