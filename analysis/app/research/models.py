"""경쟁 참가자. 모두 같은 워크포워드 분할로 학습 · 예측한다.

과제 A(종목 선택): (date, symbol) 점수 → 점수 상위 종목 보유
과제 B(시장 진입): date 별 확률/점수 → 0 초과(또는 0.5 초과)면 보유
"""
import numpy as np
import pandas as pd

from .features import COIN_FEATURES, MARKET_FEATURES

FEATURES_A = COIN_FEATURES + MARKET_FEATURES

# ---------------------------------------------------------------- 과제 A: 규칙 기반(학습 없음)
RULES_A = {
    "RULE_LOWVOL": lambda d: -d["vol_20"],          # 앞 단계에서 신호가 있었던 저변동성
    "RULE_MOM_30": lambda d: d["ret_30"],
    "RULE_REV_7": lambda d: -d["ret_7"],
}


def _fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float = 10.0):
    X1 = np.column_stack([np.ones(len(X)), X])
    reg = alpha * np.eye(X1.shape[1])
    reg[0, 0] = 0
    return np.linalg.solve(X1.T @ X1 + reg, X1.T @ y)


def _predict_ridge(beta, X):
    return np.column_stack([np.ones(len(X)), X]) @ beta


def _lgbm_params(objective: str, seed: int = 7) -> dict:
    return dict(objective=objective, learning_rate=0.03, num_leaves=15, min_data_in_leaf=300,
                feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10.0,
                verbose=-1, seed=seed, num_threads=2)


def walk_forward(dataset: pd.DataFrame, folds, model: str, target: str, features: list,
                 rounds: int = 300) -> pd.Series:
    """folds 마다 train_end 까지로 학습해 시험 구간 점수를 낸다. 정답이 없는 학습 행은 버린다."""
    dates = dataset.index.get_level_values("date")
    out = []
    for fold in folds:
        train = dataset[(dates <= fold.train_end)].dropna(subset=[target])
        test = dataset[(dates >= fold.test_start) & (dates <= fold.test_end)]
        if test.empty or len(train) < 1000:
            continue
        Xtr = train[features].to_numpy(dtype=float)
        Xte = test[features].to_numpy(dtype=float)
        ytr = train[target].to_numpy(dtype=float)
        if model == "LINEAR":
            mu = np.nanmean(Xtr, axis=0)
            beta = _fit_ridge(np.nan_to_num(Xtr - mu), ytr)
            pred = _predict_ridge(beta, np.nan_to_num(Xte - mu))
        elif model == "LGBM":
            import lightgbm as lgb
            booster = lgb.train(_lgbm_params("regression"), lgb.Dataset(Xtr, ytr), num_boost_round=rounds)
            pred = booster.predict(Xte)
        else:
            raise ValueError(model)
        out.append(pd.Series(pred, index=test.index))
    return pd.concat(out) if out else pd.Series(dtype=float)


def rule_scores(dataset: pd.DataFrame, rule: str, start, end) -> pd.Series:
    dates = dataset.index.get_level_values("date")
    d = dataset[(dates >= start) & (dates <= end)]
    return RULES_A[rule](d)


# ---------------------------------------------------------------- 과제 B: 시장 진입
def market_dataset(dataset: pd.DataFrame, index_fwd: pd.Series) -> pd.DataFrame:
    """날짜별 시장 특징 + 정답(지수의 다음 5일 수익률)."""
    m = dataset[MARKET_FEATURES].groupby(level="date").first()
    m["fwd"] = index_fwd.reindex(m.index)
    m["up"] = (m["fwd"] > 0).astype(float).where(m["fwd"].notna())
    return m


RULES_B = {
    "ALWAYS_IN": lambda m: pd.Series(1.0, index=m.index),
    "RULE_BTC_MA20": lambda m: (m["btc_ma_dist_20"] > 0).astype(float),
    "RULE_MKT_MA20": lambda m: (m["mkt_ma_dist_20"] > 0).astype(float),
    "RULE_MKT_MA50": lambda m: (m["mkt_ma_dist_50"] > 0).astype(float),
}


def walk_forward_market(m: pd.DataFrame, folds, model: str, rounds: int = 200) -> pd.Series:
    """과제 B 확률(상승일 확률). 학습 행이 적으므로(날짜당 1행) 작은 모델만 쓴다."""
    out = []
    for fold in folds:
        train = m[m.index <= fold.train_end].dropna(subset=["up"])
        test = m[(m.index >= fold.test_start) & (m.index <= fold.test_end)]
        if test.empty or len(train) < 300:
            continue
        Xtr, Xte = train[MARKET_FEATURES].to_numpy(float), test[MARKET_FEATURES].to_numpy(float)
        ytr = train["up"].to_numpy(float)
        if model == "LOGIT":
            mu, sd = np.nanmean(Xtr, 0), np.nanstd(Xtr, 0) + 1e-9
            beta = _fit_ridge(np.nan_to_num((Xtr - mu) / sd), ytr - 0.5, alpha=50.0)
            pred = 0.5 + _predict_ridge(beta, np.nan_to_num((Xte - mu) / sd))
        elif model == "LGBM":
            import lightgbm as lgb
            params = _lgbm_params("binary")
            params.update(num_leaves=7, min_data_in_leaf=60)
            booster = lgb.train(params, lgb.Dataset(Xtr, ytr), num_boost_round=rounds)
            pred = booster.predict(Xte)
        else:
            raise ValueError(model)
        out.append(pd.Series(pred, index=test.index))
    return pd.concat(out) if out else pd.Series(dtype=float)
