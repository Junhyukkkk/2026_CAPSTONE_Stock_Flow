package com.stockflow.api.alpaca.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class AlpacaQuoteMessage {

    @JsonProperty("T")
    private String type;

    @JsonProperty("S")
    private String symbol;

    @JsonProperty("c")
    private List<String> conditions;

    @JsonProperty("bp")
    private Double bidPrice;

    @JsonProperty("z")
    private String tape;

    @JsonProperty("bs")
    private Integer bidSize;

    @JsonProperty("ap")
    private Double askPrice;

    @JsonProperty("as")
    private Integer askSize;

    @JsonProperty("t")
    private String timestamp;

    @JsonProperty("bx")
    private String bidExchange;

    @JsonProperty("ax")
    private String askExchange;
}
