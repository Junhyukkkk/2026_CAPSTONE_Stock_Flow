package com.Practice.PracticeVibeCoding.alpaca.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AlpacaTradeMessage {

    @JsonProperty("T")
    private String type;  // "t" for trade

    @JsonProperty("S")
    private String symbol;  // 종목 심볼 (예: AAPL)

    @JsonProperty("p")
    private Double price;  // 체결 가격

    @JsonProperty("s")
    private Integer size;  // 체결 수량

    @JsonProperty("t")
    private String timestamp;  // 체결 시간 (ISO 8601)

    @JsonProperty("x")
    private String exchange;  // 거래소 코드

    @JsonProperty("c")
    private String[] conditions;  // 거래 조건
}
