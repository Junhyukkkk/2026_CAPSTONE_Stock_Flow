"""
종목별 가격·체결 수량 모델

로그수익률 r = mu + sigma * N(0,1) - kappa * ln(P/P0) 로 체결마다 한 걸음씩 움직인다.
 - sigma: 연환산 변동성을 체결 간 평균 시간(초)에 맞춰 환산 (거래시간 연 252*6.5*3600초 기준)
 - kappa: 시작가 P0 로의 약한 평균회귀. 시정수 5거래일 → 정상 분포 표준편차 ≈ 0.1*연변동성
 - 드물게(0.2%) 3~6배 변동성의 점프
내부 가격은 반올림하지 않고(저가주는 한 걸음이 1센트보다 작다) 출력할 때만 센트로 반올림한다.
"""
import math
import random

TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
MEAN_REVERSION_TIME_SEC = 5 * 6.5 * 3600
JUMP_PROB = 0.002
JUMP_MULT_RANGE = (3.0, 6.0)
# 극단적 변동성 입력에도 시작가에서 크게 벗어나지 않게 하는 안전장치 (약 -26% ~ +35%)
MAX_LOG_DEVIATION = 0.30

BLOCK_PROB = 0.01
BLOCK_RANGE = (1000, 20000)
LOT_SNAP_PROB = 0.4
LOT_SNAP_MIN = 50
VOLUME_LOGNORMAL_MEDIAN = 60.0
VOLUME_LOGNORMAL_SIGMA = 1.0
VOLUME_RANGE = (1, 500)


class PriceModel:
    def __init__(self, start_price: float, volatility: float, mean_interval_sec: float, rng: random.Random):
        self._rng = rng
        self._log_start = math.log(start_price)
        self._x = 0.0  # ln(P/P0)
        self._sigma = volatility * math.sqrt(mean_interval_sec / TRADING_SECONDS_PER_YEAR)
        self._mu = -0.5 * self._sigma ** 2
        self._kappa = min(0.5, mean_interval_sec / MEAN_REVERSION_TIME_SEC)

    def next_price_cents(self) -> int:
        """한 체결만큼 가격을 움직이고 센트 단위 정수 가격(≥1)을 반환한다."""
        sigma = self._sigma
        if self._rng.random() < JUMP_PROB:
            sigma *= self._rng.uniform(*JUMP_MULT_RANGE)
        x = self._x + self._mu + sigma * self._rng.gauss(0.0, 1.0) - self._kappa * self._x
        self._x = max(-MAX_LOG_DEVIATION, min(MAX_LOG_DEVIATION, x))
        return max(1, int(round(math.exp(self._log_start + self._x) * 100)))

    def next_volume(self) -> int:
        rng = self._rng
        if rng.random() < BLOCK_PROB:
            return rng.randint(*BLOCK_RANGE)
        volume = rng.lognormvariate(math.log(VOLUME_LOGNORMAL_MEDIAN), VOLUME_LOGNORMAL_SIGMA)
        volume = int(round(volume))
        if volume >= LOT_SNAP_MIN and rng.random() < LOT_SNAP_PROB:
            volume = round(volume / 100) * 100 or 100  # 라운드 로트(100주 단위)에 가깝게
        return max(VOLUME_RANGE[0], min(VOLUME_RANGE[1], volume))


def poisson(rng: random.Random, lam: float) -> int:
    """Poisson(lam) 표본. 작은 lam 은 Knuth, 큰 lam 은 정규 근사(exp 언더플로 방지)."""
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    limit = math.exp(-lam)
    k = 0
    p = rng.random()
    while p > limit:
        k += 1
        p *= rng.random()
    return k
