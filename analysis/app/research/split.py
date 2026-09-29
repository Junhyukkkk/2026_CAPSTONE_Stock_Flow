"""워크포워드 분할: 매달 그 시점까지로 다시 학습하고 다음 달을 예측한다.

정답이 앞으로 HORIZON 일 수익률이므로, 학습 마지막 날의 정답이 시험 구간 안의 가격을 쓰지 않도록
학습 · 시험 사이에 embargo 일을 비운다(학습 날짜 < 시험 시작 - embargo).
"""
from dataclasses import dataclass

import pandas as pd

from .features import HORIZON


@dataclass(frozen=True)
class Fold:
    train_end: pd.Timestamp    # 학습에 쓰는 마지막 날짜(포함)
    test_start: pd.Timestamp
    test_end: pd.Timestamp     # 포함


def monthly_folds(eval_start: str, eval_end: str, embargo: int = HORIZON) -> list:
    folds = []
    for month_start in pd.date_range(eval_start, eval_end, freq="MS"):
        month_end = min(month_start + pd.offsets.MonthEnd(0), pd.Timestamp(eval_end))
        folds.append(Fold(train_end=month_start - pd.Timedelta(days=embargo + 1),
                          test_start=month_start, test_end=month_end))
    return folds
