"""
normalizer.py 단위 테스트

중점 검증 대상:
1. Alpaca 나노초(9자리) 타임스탬프가 Python 3.9/3.10에서도 손실 없이 파싱되는지
   (Critical #1 회귀 테스트)
2. Binance 결합 스트림(envelope) 형식이 올바르게 언래핑되는지
3. Alpaca trade ID가 실제 "i" 필드를 우선 사용하고, 없을 때만 합성 ID로
   폴백하는지 (Important #1 회귀 테스트)
4. price/volume이 float이 아닌 Decimal로 보존되고, to_dict()에서 정밀도
   손실 없이 문자열로 직렬화되는지
"""
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from normalizer import DataNormalizer, NormalizedTradeDTO


# ---------------------------------------------------------------------------
# 1. Alpaca 나노초 타임스탬프 파싱 (Critical #1)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "timestamp_str, expected_dt",
    [
        # 나노초(9자리) - Python 3.9/3.10의 fromisoformat()이 원래 못 받던 케이스
        ("2023-01-01T12:00:00.123456789Z", datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)),
        # 초 단위(소수점 없음)
        ("2023-01-01T12:00:00Z", datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=timezone.utc)),
        # 밀리초(3자리)
        ("2023-01-01T12:00:00.123Z", datetime(2023, 1, 1, 12, 0, 0, 123000, tzinfo=timezone.utc)),
        # 마이크로초(6자리) - 원래도 파싱 가능했던 케이스
        ("2023-01-01T12:00:00.123456Z", datetime(2023, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)),
        # 1자리(비정상적으로 짧은 경우) - 0으로 패딩되어야 함
        ("2023-01-01T12:00:00.5Z", datetime(2023, 1, 1, 12, 0, 0, 500000, tzinfo=timezone.utc)),
    ],
)
def test_alpaca_trade_timestamp_parses_regardless_of_fractional_digits(timestamp_str, expected_dt):
    """Alpaca 'T':'t' 메시지의 다양한 소수점 자릿수 타임스탬프가 정확히 변환되는지 확인"""
    payload = {
        "T": "t",
        "S": "AAPL",
        "p": 150.25,
        "s": 100,
        "t": timestamp_str,
        "x": "IEX",
        "i": 99999,
    }

    normalized = DataNormalizer.normalize_alpaca_trade_data(payload)

    assert normalized is not None
    expected_ms = int(expected_dt.timestamp() * 1000)
    assert normalized.timestamp == expected_ms
    # 폴백(현재 처리 시각)으로 대체되지 않았는지 확인 - 실패 시 timestamp가
    # "지금"에 가까운 큰 값이 되어 2023년 값과 크게 어긋난다.
    now_ms = int(time.time() * 1000)
    assert abs(normalized.timestamp - now_ms) > 1_000 * 60 * 60 * 24 * 365  # 최소 1년 이상 차이


def test_alpaca_quote_timestamp_also_handles_nanoseconds():
    """Quote 경로(정상 동작 시 dead code지만 일관성을 위해 동일 로직 사용)도 나노초를 처리해야 함"""
    payload = {
        "S": "AAPL",
        "bp": 150.20,
        "ap": 150.30,
        "t": "2023-06-15T09:30:00.987654321Z",
        "bx": "IEX",
    }

    normalized = DataNormalizer.normalize_alpaca_quote_data(payload)

    assert normalized is not None
    expected_dt = datetime(2023, 6, 15, 9, 30, 0, 987654, tzinfo=timezone.utc)
    assert normalized.timestamp == int(expected_dt.timestamp() * 1000)


def test_alpaca_trade_timestamp_falls_back_to_processing_time_on_garbage_input(caplog):
    """진짜로 파싱 불가능한 타임스탬프는 현재 처리 시각으로 폴백하되, 경고 로그를 남겨야 함"""
    payload = {
        "T": "t",
        "S": "AAPL",
        "p": 150.25,
        "s": 100,
        "t": "not-a-valid-timestamp",
        "x": "IEX",
    }

    before_ms = int(time.time() * 1000)
    normalized = DataNormalizer.normalize_alpaca_trade_data(payload)
    after_ms = int(time.time() * 1000)

    assert normalized is not None
    # 폴백 값은 "지금"이어야 한다
    assert before_ms <= normalized.timestamp <= after_ms

    # 로그가 "실제 시각이 아니라 처리 시각으로 대체했다"는 사실을 명확히 알려야 함
    warning_messages = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert any("처리 시각" in msg for msg in warning_messages)


# ---------------------------------------------------------------------------
# 2. Binance 결합 스트림(envelope) 언래핑
# ---------------------------------------------------------------------------

def test_binance_combined_stream_envelope_is_unwrapped():
    """{"stream": ..., "data": {...}} 형식이 올바르게 언래핑되어 정규화되는지 확인"""
    envelope = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 12345,
            "p": "50000.12345678",
            "q": "1.5",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": True,
        },
    }

    normalized = DataNormalizer.normalize_binance_data(envelope)

    assert normalized is not None
    assert normalized.symbol == "BTCUSDT"
    assert normalized.price == Decimal("50000.12345678")
    assert normalized.volume == Decimal("1.5")
    assert normalized.trade_id == "12345"
    assert normalized.exchange == "BINANCE"
    assert normalized.timestamp == 123456785
    assert normalized.market_type == "CRYPTO"


def test_binance_single_message_format_without_envelope_still_works():
    """envelope 없이 단일 메시지 형식으로 와도 정규화되어야 함 (하위 호환)"""
    raw = {
        "e": "aggTrade",
        "E": 123456789,
        "s": "ETHUSDT",
        "a": 54321,
        "p": "3000.5",
        "q": "2.0",
        "f": 200,
        "l": 205,
        "T": 987654321,
        "m": False,
    }

    normalized = DataNormalizer.normalize_binance_data(raw)

    assert normalized is not None
    assert normalized.symbol == "ETHUSDT"
    assert normalized.trade_id == "54321"


def test_binance_missing_required_field_returns_none():
    """필수 필드가 없으면 None을 반환해야 함"""
    incomplete = {"data": {"s": "BTCUSDT", "p": "50000"}}  # q, T, a 누락
    assert DataNormalizer.normalize_binance_data(incomplete) is None


# ---------------------------------------------------------------------------
# 3. Alpaca trade_id: 실제 "i" 필드 우선, 없으면 합성 ID로 폴백
# ---------------------------------------------------------------------------

def test_alpaca_trade_id_uses_real_i_field_when_present():
    payload = {
        "T": "t",
        "S": "AAPL",
        "p": 150.25,
        "s": 100,
        "t": "2023-01-01T12:00:00Z",
        "x": "IEX",
        "i": 123456789012345,
    }

    normalized = DataNormalizer.normalize_alpaca_trade_data(payload)

    assert normalized is not None
    assert normalized.trade_id == "123456789012345"


def test_alpaca_trade_id_falls_back_to_synthetic_id_when_i_missing():
    payload = {
        "T": "t",
        "S": "AAPL",
        "p": 150.25,
        "s": 100,
        "t": "2023-01-01T12:00:00Z",
        "x": "IEX",
        # "i" 필드 없음
    }

    normalized = DataNormalizer.normalize_alpaca_trade_data(payload)

    assert normalized is not None
    expected_id = f"IEX_{normalized.timestamp}_AAPL_100"
    assert normalized.trade_id == expected_id


def test_alpaca_trade_id_distinguishes_distinct_trades_with_same_size():
    """같은 size(s)를 가진 서로 다른 두 체결이 'i'가 있으면 서로 다른 trade_id를 가져야 함
    (fabricated ID가 size 기반이라 충돌하던 버그의 회귀 테스트)"""
    base_payload = {
        "T": "t",
        "S": "AAPL",
        "p": 150.25,
        "s": 100,
        "t": "2023-01-01T12:00:00Z",
        "x": "IEX",
    }
    trade_1 = DataNormalizer.normalize_alpaca_trade_data({**base_payload, "i": 111})
    trade_2 = DataNormalizer.normalize_alpaca_trade_data({**base_payload, "i": 222})

    assert trade_1.trade_id != trade_2.trade_id
    assert trade_1.trade_id == "111"
    assert trade_2.trade_id == "222"


# ---------------------------------------------------------------------------
# 4. price/volume은 Decimal로 보존되고, to_dict()는 문자열로 직렬화
# ---------------------------------------------------------------------------

def test_binance_price_and_volume_are_decimal_not_float():
    raw = {
        "e": "aggTrade",
        "s": "BTCUSDT",
        "a": 1,
        # float으로 변환되면 정밀도가 깨지기 쉬운 값
        "p": "0.00000001",
        "q": "123456789.123456789",
        "T": 123,
    }

    normalized = DataNormalizer.normalize_binance_data(raw)

    assert isinstance(normalized.price, Decimal)
    assert isinstance(normalized.volume, Decimal)
    assert normalized.price == Decimal("0.00000001")
    assert normalized.volume == Decimal("123456789.123456789")


def test_alpaca_price_and_volume_are_decimal_not_float():
    payload = {
        "T": "t",
        "S": "AAPL",
        "p": "150.123456789",
        "s": "100.5",
        "t": "2023-01-01T12:00:00Z",
        "x": "IEX",
        "i": 1,
    }

    normalized = DataNormalizer.normalize_alpaca_trade_data(payload)

    assert isinstance(normalized.price, Decimal)
    assert isinstance(normalized.volume, Decimal)


def test_to_dict_serializes_price_and_volume_as_precision_preserving_strings():
    # float으로 거쳤다면 배정밀도 반올림 오차가 발생하기 쉬운, 유효숫자가 많은 값
    price = Decimal("50000.123456789012345678")
    volume = Decimal("123456789.123456789")

    dto = NormalizedTradeDTO(
        source="BINANCE",
        symbol="BTCUSDT",
        price=price,
        volume=volume,
        trade_id="1",
        exchange="BINANCE",
        timestamp=123,
        received_at=124,
        market_type="CRYPTO",
    )

    result = dto.to_dict()

    assert isinstance(result["price"], str)
    assert isinstance(result["volume"], str)
    # float(price)였다면 이 자리에서 유효숫자가 소실된다 (float은 15~17자리가 한계)
    assert Decimal(result["price"]) == price
    assert Decimal(result["volume"]) == volume
    assert float(price) != price  # float 변환 시 정밀도가 실제로 달라짐을 증명

    # JSON으로 직렬화 가능해야 함 (Kafka 전송 전 단계) - 문자열로 왕복되어야 함
    json_str = dto.to_json()
    round_tripped = json.loads(json_str)
    assert isinstance(round_tripped["price"], str)
    assert Decimal(round_tripped["price"]) == price
