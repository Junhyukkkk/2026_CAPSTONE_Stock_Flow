"""연구 틀의 누수 방지 · 분할 · 봉인 · 백테스트 계산 검증(합성 데이터, DB 불필요)."""
import unittest
from datetime import date

import numpy as np
import pandas as pd

from app.research import backtest as bt
from app.research.data import HOLDOUT_START, Panel, load_panel
from app.research.features import HORIZON, build_dataset
from app.research.split import monthly_folds
from app.research.universe import point_in_time_universe


def synthetic_panel(days=400, n=40, seed=1):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2022-01-01", periods=days, freq="D")
    cols = [f"C{i:02d}USDT" for i in range(n - 1)] + ["BTCUSDT"]
    close = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.03, (days, n)), axis=0)), idx, cols)
    high, low = close * 1.02, close * 0.98
    volume = pd.DataFrame(rng.uniform(1e3, 1e5, (days, n)), idx, cols)
    return Panel(open=close.shift(1).fillna(close), high=high, low=low, close=close, volume=volume)


class NoLookaheadTest(unittest.TestCase):
    def test_features_and_universe_at_t_ignore_future_prices(self):
        p = synthetic_panel()
        cut = p.close.index[300]
        future = p.close.index > cut
        q = Panel(**{k: getattr(p, k).copy() for k in ("open", "high", "low", "close", "volume")})
        for frame in (q.close, q.high, q.low, q.open):
            frame.loc[future] *= 3.0          # 미래 가격을 크게 바꾼다
        q.volume.loc[future] *= 0.1

        up, uq = point_in_time_universe(p, top_n=20), point_in_time_universe(q, top_n=20)
        pd.testing.assert_frame_equal(up.loc[:cut], uq.loc[:cut])

        dp, dq = build_dataset(p, up), build_dataset(q, uq)
        feats = [c for c in dp.columns if c not in ("fwd", "fwd_rank")]
        early = lambda d: d[d.index.get_level_values("date") <= cut - pd.Timedelta(days=HORIZON)]
        pd.testing.assert_frame_equal(early(dp)[feats], early(dq)[feats])
        # 정답은 미래를 봐야 하므로 cut 근처에서 달라져야 정상
        pd.testing.assert_frame_equal(early(dp)[["fwd"]], early(dq)[["fwd"]])
        near = lambda d: d[d.index.get_level_values("date") == cut]["fwd"]
        self.assertFalse(np.allclose(near(dp).to_numpy(), near(dq).to_numpy()))


class SplitAndSealTest(unittest.TestCase):
    def test_monthly_folds_leave_an_embargo_gap(self):
        for f in monthly_folds("2024-01-01", "2024-06-30"):
            # 학습 마지막 날의 정답(+HORIZON 일)이 시험 시작 전에 끝나야 한다
            self.assertLess(f.train_end + pd.Timedelta(days=HORIZON), f.test_start)
            self.assertEqual(f.test_start.day, 1)

    def test_holdout_is_sealed_by_default(self):
        with self.assertRaises(ValueError):
            load_panel(end=HOLDOUT_START)
        with self.assertRaises(ValueError):
            load_panel(end=date(2026, 9, 1))


class BacktestTest(unittest.TestCase):
    def test_top_k_charges_costs_and_uses_next_day_returns(self):
        idx = pd.date_range("2024-01-01", periods=4, freq="D")
        close = pd.DataFrame({"A": [100, 110, 121, 133.1], "B": [100, 100, 100, 100]}, idx)
        scores = pd.Series({(idx[0], "A"): 1.0, (idx[0], "B"): 0.0,
                            (idx[1], "A"): 1.0, (idx[1], "B"): 0.0,
                            (idx[2], "A"): 1.0, (idx[2], "B"): 0.0})
        scores.index.names = ["date", "symbol"]
        r = bt.top_k_portfolio(scores, close, k=1, cost=0.001, rebalance=5)
        # 첫날 A 매수(비용 0.1%), 이후 매일 +10%
        self.assertAlmostEqual(r.iloc[0], 0.10 - 0.001, places=9)
        self.assertAlmostEqual(r.iloc[1], 0.10, places=9)

    def test_sparse_and_daily_scores_rebalance_on_same_calendar(self):
        # Chronos 처럼 매매일에만 점수가 있어도, 매일 점수가 있는 모델과 같은 날 매매해야 공정하다
        idx = pd.date_range("2024-01-01", periods=12, freq="D")
        rng = np.random.default_rng(3)
        close = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.05, (12, 3)), 0)), idx, list("ABC"))
        daily_rank = {"A": 3.0, "B": 2.0, "C": 1.0}
        daily = pd.Series({(t, s): daily_rank[s] + (i % 5 != 0) * (10 if s == "C" else 0)
                           for i, t in enumerate(idx) for s in "ABC"})
        daily.index.names = ["date", "symbol"]
        sparse = daily[[d in idx[::5] for d in daily.index.get_level_values("date")]]
        a = bt.top_k_portfolio(daily, close, k=1, rebalance=5, calendar_start=idx[0])
        b = bt.top_k_portfolio(sparse, close, k=1, rebalance=5, calendar_start=idx[0])
        pd.testing.assert_series_equal(a.loc[b.index], b)

    def test_tranched_averages_offset_calendars(self):
        idx = pd.date_range("2024-01-01", periods=10, freq="D")
        calls = []

        def strategy(cal_start):
            calls.append((cal_start - idx[0]).days)
            return pd.Series(float((cal_start - idx[0]).days), index=idx)

        r = bt.tranched(strategy, idx[0], tranches=5)
        self.assertEqual(calls, [0, 1, 2, 3, 4])
        self.assertTrue((r == 2.0).all())

    def test_timing_holds_cash_when_signal_off(self):
        idx = pd.date_range("2024-01-01", periods=3, freq="D")
        asset = pd.Series([0.0, 0.05, 0.05], idx)
        r = bt.timing_strategy(asset, pd.Series([0.0, 1.0, 1.0], idx), cost=0.0, rebalance=1)
        self.assertEqual(list(r.round(6)), [0.0, 0.05])


if __name__ == "__main__":
    unittest.main()
