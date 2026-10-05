# Grafana 패널이 비거나 끊길 때

대시보드 정의: `backend/docs/0*-*.json` (7종), Prometheus 스크레이프: `backend/infra/prometheus/prometheus.yml`.
Grafana 는 터널로 접속한다 → [RUNBOOK.md §1](../../RUNBOOK.md).

## 빠른 참조 (증상 → 원인 → 조치)

| 증상 | 원인 | 조치 |
|---|---|---|
| Kafka / Redis / 호스트 CPU·메모리 패널 No data | exporter 4종(kafka·redis·node·cAdvisor)은 `metrics` 프로파일에서만 뜬다. 안 켜면 타깃이 down | `cd backend/infra && docker compose --profile metrics up -d` (CD 배포 `deploy.yml` 은 이 프로파일을 포함) |
| "거래소→화면 지연"(E2E)이 비거나 0 | 서버 시계가 NTP 동기화되지 않아 거래소 시각이 서버 시계보다 미래 → 음수 표본. 코드가 음수 표본을 버리고 `stockflow_e2e_latency_discarded_total` 을 올린다 | 서버 시각 동기화(`timedatectl`/`chrony`, 관리자 권한 필요). 실제 처리는 정상 |
| Consumer Lag·유실 위험 대시보드 통째로 빔 | 서버가 `stockflow_consumer_lag` · `stockflow_consumer_retention_margin` · 경보 규칙(`prometheus/rules/consumer_lag.yml`) 이 없는 구버전 | `main` 배포 후 `curl -s localhost:8081/actuator/prometheus \| grep stockflow_consumer_lag` 로 지표 확인 |
| 앱 패널이 구간마다 끊김 | 앱 단일 타깃을 5초(`scrape_interval: 5s`)로 긁는데, 앱이 길게 멈추거나 재시작하면 그 구간이 비어 보임. | 재시작·GC 로그와 시각 대조. 배포 직후면 정상 |
| 컨테이너 재생성 후 응답 지연 지표가 올라감 | 분석 컨테이너 CPU 예약이 풀림 | `backend/infra/reserve-analysis-capacity.sh` ([RUNBOOK.md §3](../../RUNBOOK.md)) |

타깃 상태는 http://localhost:9090/targets 에서 본다(터널 필요).
