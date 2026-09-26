# market-data-sync

캔들 데이터(1분봉 · 일봉)를 한 곳(`ohlcv_1m`, `symbol_daily_ohlcv`)에 모으고 빈 구간을 채우는 서비스.

> 원칙: **"지금"은 수집한 실시간 체결로, "과거 기록"은 거래소 공식 캔들로 확정한다.**

| 작업 | 주기 | 내용 | 상태 |
|---|---|---|---|
| 실시간 동기화 | 1분 (넓은 구간은 매시 7분) | `market_ticks_1m`의 확정된 분 → `ohlcv_1m` (LIVE) | P1 |
| 초기 백필 | 1회 (수동) | 거래소 과거 캔들 → EXCHANGE | P2 (코인) |
| 결측 복구 | 매시 20분 | 최근 6시간에 봉이 모자란 종목만 창 전체를 거래소 REST 로 다시 받음 | P3 |
| 일일 확정 | 매일 01:30 UTC | 전날 1분봉 · 일봉을 거래소 공식본으로 덮어씀(일부만 집계된 분도 교정) | P3 |

- 결측 복구는 빈 분을 하나씩 요청하지 않는다. 거래가 적은 코인은 체결 없는 분이 많아 수집 집계에는 봉이 없지만
  거래소는 거래량 0 봉을 주므로, 빈 분마다 요청하면 호출이 폭증한다(운영 측정: 48시간 · 456종목에서 38만 분).
  종목별 봉 수만 세고(약 1초), 모자란 종목은 창 전체를 한 번에 받는다(시간당 최대 약 450회).
- 일일 확정은 Spring 일봉 배치(01:05 UTC) 뒤에 돈다. Spring 배치도 `origin = 'EXCHANGE'` 행은 덮어쓰지 않는다.
- 대상은 Binance 에서 현재 거래 중인 종목뿐이다(상장폐지 · 거래중지 종목은 채울 수 없다).

`origin` 규칙: EXCHANGE 가 있는 분은 LIVE 가 덮어쓰지 못한다. LIVE 끼리는 값이 바뀐 경우에만 갱신한다.

스키마는 Spring Flyway 마이그레이션 `V16__ohlcv_1m_and_data_coverage.sql` 이 만든다.

## 실행

`backend/infra` 스택이 떠 있어 `infra_default` 네트워크와 V16 이 적용된 DB 가 있어야 한다.

```bash
cd market-data-sync
docker compose up -d --build
```

과거 구간을 한 번에 채울 때(연속 집계에 남아 있는 구간):

```bash
docker compose run --rm market-data-sync python -m app.live_sync --from 2026-03-29 --to 2026-09-26
```

## 초기 백필 (거래소 공식 캔들 → EXCHANGE)

Binance 는 API 키가 필요 없다. 1분봉은 `data.binance.vision` 월별 zip(SHA256 검증)을,
일봉은 REST `/api/v3/klines` 를 쓴다. 아카이브 시각은 2025 년부터 마이크로초 단위라 자동 판별한다.

```bash
# 코인 일봉: 거래 중인 USDT 전 종목 + DB 에 이미 있는 종목
docker compose run --rm market-data-sync python -m app.backfill daily --symbols ALL --since 2017-01-01

# 코인 1분봉: 완료된 달은 월별 파일, 이번 달은 어제까지 일별 파일
docker compose run --rm market-data-sync python -m app.backfill minute --symbols BTCUSDT,ETHUSDT --from 2022-01

# 최근 30일 거래대금 상위 100개(스테이블·래핑 토큰 제외) — 일봉 백필을 먼저 해야 한다
docker compose run --rm market-data-sync python -m app.backfill minute --symbols TOP:100 --from 2022-01
```

실측(BTC, 2022-01~): 1분봉 249만 행 285초, 압축 전 544MB → 후 126MB. 100종목이면 압축 후 최대 약 12.6GB,
순차 약 8시간이다. 결측 80분은 2023-03-24 Binance 현물 거래 중단 구간이라 거래소에도 데이터가 없다.

> 주의: `TOP:100` 은 **현재** 살아 있는 종목만 고르므로 상장폐지된 코인이 빠지는 생존 편향이 있다.
> 종목 간 비교 연구에는 일봉(`ALL`)을 쓰는 편이 낫다.

종목마다 `data_coverage` 에 보유 범위와 결측 수가 기록된다. 한 종목이 실패해도 나머지는 계속 진행하고,
같은 명령을 다시 돌리면 바뀐 행만 갱신된다.

## 결측 복구 · 일일 확정 수동 실행

```bash
# 서버가 오래 멈췄던 뒤: 검사 범위를 넓혀 한 번 돌린다
docker compose run --rm market-data-sync python -m app.repair repair --hours 48

# 특정 날짜를 거래소 공식본으로 다시 확정
docker compose run --rm market-data-sync python -m app.repair confirm --day 2026-08-20
```

## 테스트

```bash
python -m unittest discover -s tests          # 단위 테스트 (DB 불필요)
MDS_INTEGRATION_DB=1 DB_HOST=... python -m unittest tests.test_live_sync_db   # 일회용 DB 필요
```
