package com.Practice.PracticeVibeCoding.alpaca.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class AlpacaQuoteMessage {

    @JsonProperty("T")
    private String type;  // "q" for quote

    @JsonProperty("S")
    private String symbol;  // 종목 심볼 (예: AAPL)

    @JsonProperty("c")
    private List<String> conditions; // 이 부분 추가

    @JsonProperty("bp")
    private Double bidPrice;  // 매수 호가

    @JsonProperty("z")
    private String tape;

    @JsonProperty("bs")
    private Integer bidSize;  // 매수 수량

    @JsonProperty("ap")
    private Double askPrice;  // 매도 호가

    @JsonProperty("as")
    private Integer askSize;  // 매도 수량

    @JsonProperty("t")
    private String timestamp;  // 시간 (ISO 8601)

    @JsonProperty("bx")
    private String bidExchange;  // 매수 거래소

    @JsonProperty("ax")
    private String askExchange;  // 매도 거래소
}
