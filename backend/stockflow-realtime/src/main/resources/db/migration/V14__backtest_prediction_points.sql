-- 예측 기반 백테스트의 날짜별 모델 예측값과 신호를 실행 결과와 함께 보존한다.
CREATE TABLE backtest_prediction_points (
    run_id              BIGINT         NOT NULL REFERENCES backtest_runs (id) ON DELETE CASCADE,
    signal_date         DATE           NOT NULL,
    execution_date      DATE           NOT NULL,
    reference_price     NUMERIC(24, 8) NOT NULL,
    predicted_price     NUMERIC(24, 8) NOT NULL,
    expected_return_pct NUMERIC(16, 6) NOT NULL,
    threshold_pct       NUMERIC(16, 6) NOT NULL,
    signal              VARCHAR(8)     NOT NULL
        CONSTRAINT chk_backtest_prediction_point_signal CHECK (signal IN ('BUY', 'HOLD', 'SELL')),
    PRIMARY KEY (run_id, execution_date)
);

COMMENT ON TABLE backtest_prediction_points IS
    '예측 기반 백테스트의 일자별 예측가, 기준가, 신호 및 판단 임계값.';
