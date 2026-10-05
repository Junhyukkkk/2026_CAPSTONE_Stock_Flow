"""simulator.universe.load_universe 단위 테스트"""
from simulator.universe import load_universe


def _write(tmp_path, body):
    path = tmp_path / "universe.csv"
    path.write_text(body, encoding="utf-8")
    return str(path)


def test_loads_valid_rows(tmp_path):
    path = _write(tmp_path, "symbol,price,volatility,weight\nAAPL,230.5,0.25,10\nmsft,420,0.22,8\n")
    instruments = load_universe(path)
    assert [(i.symbol, i.price, i.volatility, i.weight) for i in instruments] == [
        ("AAPL", 230.5, 0.25, 10.0),
        ("MSFT", 420.0, 0.22, 8.0),
    ]


def test_skips_malformed_and_out_of_range_rows(tmp_path, caplog):
    path = _write(tmp_path, "\n".join([
        "symbol,price,volatility,weight",
        "GOOD,100,0.2,1",
        "BADPRICE,abc,0.2,1",
        "MISSING,100,0.2",          # weight 열 없음
        "ZEROPRICE,0,0.2,1",
        "NEGPRICE,-5,0.2,1",
        "ZEROWEIGHT,100,0.2,0",
        "NEGWEIGHT,100,0.2,-1",
        "NEGVOL,100,-0.2,1",
        "NANPRICE,nan,0.2,1",
        ",100,0.2,1",               # 심볼 없음
        "GOOD,200,0.2,1",           # 중복 심볼
        "LAST,50,0.3,2",
        "",
    ]))
    with caplog.at_level("WARNING"):
        instruments = load_universe(path)

    assert [i.symbol for i in instruments] == ["GOOD", "LAST"]
    assert instruments[0].price == 100.0
    assert "BADPRICE" in caplog.text
    assert "ZEROPRICE" in caplog.text


def test_daily_trades_is_optional_and_parsed(tmp_path):
    path = _write(tmp_path, "symbol,price,volatility,weight,daily_trades\nAAPL,230,0.25,10,900000\nMSFT,420,0.22,8,\n")
    by_symbol = {i.symbol: i for i in load_universe(path)}
    assert by_symbol["AAPL"].daily_trades == 900000.0
    assert by_symbol["MSFT"].daily_trades is None  # 빈 값 → weight 폴백

    legacy = _write(tmp_path, "symbol,price,volatility,weight\nAAPL,230,0.25,10\n")  # 열 자체가 없는 구형 CSV
    assert load_universe(legacy)[0].daily_trades is None


def test_invalid_daily_trades_ignores_only_that_value_and_keeps_row(tmp_path, caplog):
    path = _write(tmp_path, "\n".join([
        "symbol,price,volatility,weight,daily_trades",
        "A,100,0.2,1,abc",
        "B,100,0.2,1,0",
        "C,100,0.2,1,-5",
        "D,100,0.2,1,nan",
        "E,100,0.2,1,inf",
        "F,100,0.2,1,250000",
        "",
    ]))
    with caplog.at_level("WARNING"):
        instruments = load_universe(path)
    assert [i.symbol for i in instruments] == list("ABCDEF")
    assert [i.daily_trades for i in instruments] == [None] * 5 + [250000.0]
    assert [i.weight for i in instruments] == [1.0] * 6
    assert caplog.text.count("daily_trades") >= 5
