# 서버 처리량(TPS) 측정 — 실행 순서

학교 서버에서 `git pull` → `docker compose` 로 스택을 올리고, rate 스윕으로
"이 서버는 초당 몇 건까지 소화하는가 + 넘으면 뭐가 병목인가"를 잰다.

관련 배경: `발표자료.md` §9(부하 안정성), 8/5 미팅(`docs/권재욱, 최준혁 캡스톤(2026).md`).

---

## 0. 사전 (서버에서 1회)

```bash
cd ~/capstone            # git 받아둔 경로
git fetch && git checkout perf/server-tps-sweep && git pull

cd backend/infra
cp -n .env.example .env

# 스택 기동. 서버가 구버전이면 구간 계측(stockflow_stage_seconds)·
# consumer_lag 지표가 안 붙으므로 --build 필수.
docker compose up -d --build

# (선택) Grafana 의 kafka/redis/host 패널 채우기
docker compose --profile metrics up -d

# 헬스 확인
curl -s localhost:8081/actuator/health      # {"status":"UP"}
docker compose ps
```

---

## 1. 스윕 실행

```bash
cd ../perf

# (a) 현재 파티션 그대로 — "지금 이 서버의 상한"
./tps-sweep.sh p6

# (b) 파티션 12로 늘려 재측정 — "설정만 고치면 얼마나 오르나"
#     (파티션은 늘리기만 가능. 되돌리려면 토픽 삭제 필요 — 부하 테스트라 무방)
./tps-sweep.sh p12 12
```

기본 스윕: `2000 3000 4000 4300 5000 6000 7000 8000` msg/s, 각 3분.
한 번에 약 40~50분. 조정:

```bash
RATES="3000 4000 5000" HOLD=120 ./tps-sweep.sh quick
WORKERS=8 ./tps-sweep.sh p6            # 부하 생성기 프로세스 수 (고 rate 에서 8까지)
STORAGE=off ./tps-sweep.sh rt-only     # 저장 Consumer 끄고 실시간 경로만
```

실행 중 다른 터미널에서:
```bash
watch -n2 'docker exec stockflow-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --group realtime-group --describe'
```

---

## 2. 결과

```
perf/results/sweep_p6_<timestamp>/
├── SUMMARY.md          ← 여기부터 본다 (rate별 판정 + 병목 + 결론)
├── summary.csv
├── timeline.csv        ← 5초 간격 lag/CPU/redis 표본 (그래프용)
├── env.txt             ← 서버 스펙, 파티션 수, 코드 리비전
├── <rate>_A.*  <rate>_B.*   ← rate 구간 앞/뒤 스냅샷 (prom/lag/redis/pg/stats)
└── <rate>_loadgen.log
```

`SUMMARY.md` 판정 값:
- **KEPT_UP** — 부하 중 소비율이 전송률을 따라갔고 종료 후 lag 배수됨 (이 서버가 소화 가능)
- **MARGINAL** — 따라가긴 하나 여유 없음
- **SATURATED** — lag 이 계속 증가, 종료 후에도 안 빠짐 (용량 초과)

"지속 가능 TPS ≈ 가장 큰 KEPT_UP rate".

---

## 3. 최신 코드 + 12파티션으로 재측정 (Phase 2)

서버가 구버전이면 `phase2.sh` 로 최신 `main` 이미지를 빌드해 교체한다 (되돌릴 수 있음).

```bash
cd backend/perf
./phase2.sh build      # origin/main 을 worktree 로 받아 stockflow-realtime:main 빌드 (~5분)
./phase2.sh deploy     # 토픽 12파티션 + 컨테이너 교체 (구 컨테이너는 -p1 로 보존)
./tps-sweep.sh p12main # 재측정
./phase2.sh restore    # 원래 컨테이너로 복구 (토픽 파티션은 12 유지 — 축소 불가)
```

## 4. 유실(retention cliff) 재현 — 선택

8/5 미팅 실험 B를 4시간 대신 몇 분으로 재현한다.
retention.ms 를 잠깐 5분으로 바꿨다가 원복하므로 **끝까지 실행**할 것.

```bash
./retention-cliff-test.sh
# → results/cliff_<ts>/RESULT.md : 투입 N건 중 저장 증가분, offset reset 로그
```

---

## 5. Grafana 패널이 비는 문제

원인·조치는 [GRAFANA_GAPS.md](GRAFANA_GAPS.md) 참조. 요약: exporter 부재 + e2e 지표
음수(서버 시계 skew) + 구버전 지표 누락.

## 6. 결과를 원격에서 분석시키려면

```bash
cd ~/capstone && git add -f backend/perf/results/sweep_* backend/perf/results/cliff_* && \
  git commit -m "perf: 서버 스윕 결과" && git push
```
(`results/` 는 평소 git 무시라 `-f` 필요)
