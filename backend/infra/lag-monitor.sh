#!/bin/bash

START_TIME=$(date +%s)
echo "⏱️ 측정 시작... (두 그룹의 totalLag이 모두 0이 되면 Ctrl+C를 누르세요)"

trap '
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo -e "\n============================="
    echo "✅ 측정 종료!"
    echo "총 소요 시간: ${DURATION}초"
    echo "============================="
    exit
' INT

while true; do
    # API 호출해서 결과 받아오기
    RESPONSE=$(curl -s http://localhost:8081/api/metrics/consumer-lag)
    
    # jq가 설치되어 있다면 JSON을 예쁘게 파싱해서 totalLag만 보여줍니다.
    # (jq가 없으면 그냥 grep으로 숫자만 빼옵니다)
    REALTIME_LAG=$(echo $RESPONSE | grep -o '"totalLag":[0-9]*' | head -1 | cut -d':' -f2)
    STORAGE_LAG=$(echo $RESPONSE | grep -o '"totalLag":[0-9]*' | tail -1 | cut -d':' -f2)

    echo "$(date '+%H:%M:%S') | Realtime Lag: ${REALTIME_LAG} | Storage Lag: ${STORAGE_LAG}"
    
    sleep 2
done
