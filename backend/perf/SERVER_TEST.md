# 서버 처리량(TPS) 측정 — 실행 가이드

이 서버가 초당 몇 건까지 소화하는가, 넘으면 병목이 무엇인가를 rate 스윕으로 잰다.
측정 결과·최적화 효과는 [OPTIMIZATION_HISTORY.md](OPTIMIZATION_HISTORY.md).
모든 명령은 서버의 `~/capstone/backend/perf` 에서 실행한다 (스택이 이미 떠 있어야 함).

## 빠른 참조 (하려는 일 → 명령)

| 하려는 일 | 명령 | 결과 위치 |
|---|---|---|
| 지금 이 서버의 상한 | `LOADGEN_MODE=sim ./tps-sweep.sh <라벨>` | `results/sweep_<라벨>_<ts>/SUMMARY.md` |
| 부하 중 예측 API 응답시간 | 위 명령에 `ANALYSIS_PROBE=1` 추가 | 같은 폴더 `analysis_probe.csv` + SUMMARY |
| 특정 rate 만 | `RATES="4000 8000" HOLD=120 ./tps-sweep.sh quick` | 〃 |
| 코드 시점(git ref)별 상한 비교 | `./revision-sweep.sh <라벨> <ref>=<rate,...> ...` | `results/revisions_<라벨>_<ts>/` |
| env 플래그 하나씩 꺼서 기여도 | `./opt-flag-sweep.sh <라벨> <rate> <STOCKFLOW_OPT_...> ...` | `results/optflags_<라벨>_<ts>/` |
| 파티션 수 변경 후 재측정 | `./tps-sweep.sh p12 12` (늘리기만 가능) | 〃 |
| 보관기간 초과 유실 재현 | `./retention-cliff-test.sh` | `results/cliff_<ts>/RESULT.md` |

`results/` 는 git 에 안 올라간다(측정 때마다 새로 생성).

## 부하 종류

| 모드 | 설정 | 용도 |
|---|---|---|
| 시뮬레이터 (권장) | `LOADGEN_MODE=sim` | 미국 주식 105종목 `SIM_RATE_MODE=realistic`, `source=SIMLOAD`. 실제 종목·가격대·Poisson 도착 |
| 합성 | (기본) `loadgen.py` | 가짜 50종목 균등 부하. `WORKERS=8` 까지 |

시뮬레이터 모드 주의:
- 라이브 시뮬레이터(`stockflow-stock-simulator`)·수집기는 스윕이 자동으로 멈췄다 되돌린다(`STOP_COLLECTORS=1`).
- 코드는 `SIM_CODE_DIR`(기본 서버의 `/home/capstone01/sim-code/collectors`)에서 읽는다. `SIM_RATE_SCALE = rate / SIM_BASE_TPS`(기본 832.479).
- 분석용으로 예약한 코어를 피하려면 `SIM_CPUSET=0-5`.
- 끝나면 `SIMLOAD` 데이터를 지운다 → [RUNBOOK.md §8](../../RUNBOOK.md).

## 사전 (서버에서 1회)

```bash
cd ~/capstone && git fetch && git reset --hard origin/main
cd backend/infra && cp -n .env.example .env
docker compose up -d --build
./reserve-analysis-capacity.sh            # 분석=코어 6,7·3GB, 나머지=코어 0-5 (컨테이너 재생성 후마다 재실행)
curl -s localhost:8081/actuator/health    # {"status":"UP"}
```

## 스윕 옵션 (`tps-sweep.sh`)

기본 `RATES="2000 3000 4000 4300 5000 6000 7000 8000"`, `HOLD=180`(rate 당 유지 초), 한 번에 약 40~50분.

```bash
RATES="4000 8000 12000" HOLD=90 LOADGEN_MODE=sim ANALYSIS_PROBE=1 ./tps-sweep.sh sim1
```

실행 중 다른 터미널에서 적체 보기:
```bash
watch -n2 'docker exec stockflow-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --group realtime-group --describe'
```

## 결과 읽기

```
results/sweep_<라벨>_<ts>/
├── SUMMARY.md       ← 여기부터 (rate별 판정 + 병목 + 결론, sweep_report.py 가 생성)
├── summary.csv  timeline.csv (주기 표본: lag/CPU/redis, epoch=서버 시계)  env.txt (서버 스펙·파티션·리비전)
├── analysis_probe.csv   (ANALYSIS_PROBE=1)
└── <rate>_A.* <rate>_B.* <rate>_loadgen.log
```

`SUMMARY.md` 판정:
- **KEPT_UP** — 부하 중 소비율이 전송률을 따라갔고 종료 후 lag 배수됨
- **MARGINAL** — 따라가나 여유 없음
- **SATURATED** — lag 계속 증가, 종료 후에도 안 빠짐

지속 가능 TPS ≈ 가장 큰 KEPT_UP rate. "소비"는 평균이라 램프업이 섞여 전송량의 88~90% 로 보이므로 소비율 + peak lag + 배수 시간으로 판단한다.

## 시점별 비교 (`revision-sweep.sh`)

ref 마다 그 시점 코드로 앱 이미지를 빌드해 운영 `stockflow-realtime` 자리에 바꿔 끼우고 같은 부하를 준 뒤 원래 컨테이너로 복구한다(Ctrl-C 포함).

```bash
DROP_ENV='^(STOCKFLOW_OPT_STORAGE_IDEMPOTENCY_PIPELINE|KAFKA_CONSUMER_MAX_POLL_RECORDS)=' \
APP_CPUSET=0-5 LOADGEN_MODE=sim \
./revision-sweep.sh march-to-now 1f814a5=2000,4000,6000 30c7a5a=4000,8000,10000
```

- `DROP_ENV`: 운영 env 에서 뺄 줄(정규식). 옛 고정값이 새 기본값을 덮어쓰지 않게 한다.
- `APP_CPUSET`: 시험 앱 컨테이너를 고정할 코어(분석 예약 코어 회피).
- 시점마다 컨슈머 오프셋을 최신으로 리셋해 lag 0 에서 출발한다.

## 최신 main + 12파티션 교체 (`phase2.sh`)

```bash
./phase2.sh build      # origin/main 을 worktree 로 받아 stockflow-realtime:main 빌드 (~5분)
./phase2.sh deploy     # 토픽 12파티션 + 컨테이너 교체 (구 컨테이너는 -p1 로 보존)
./tps-sweep.sh p12main
./phase2.sh restore    # 원래 컨테이너 복구 (파티션은 12 유지 — 축소 불가)
```
서버 경로(`REPO`, `WT`)가 하드코딩돼 있어 다른 머신에서는 환경변수로 override.

## 유실(retention cliff) 재현 — 선택

`retention.ms` 를 잠깐 5분으로 바꿨다가 원복하므로 **끝까지 실행**할 것.
```bash
./retention-cliff-test.sh   # → results/cliff_<ts>/RESULT.md : 투입 N건 중 저장 증가분, offset reset 로그
```

## Grafana 패널이 비는 문제

[GRAFANA_GAPS.md](GRAFANA_GAPS.md) 참조.
