FROM python:3.9-slim

WORKDIR /app

# 시스템 패키지 업데이트 및 필수 도구 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 헬스체크: 각 producer가 healthcheck.py 참조
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python healthcheck.py

# 기본 명령어 (docker-compose에서 override)
CMD ["python", "binance_producer.py"]
