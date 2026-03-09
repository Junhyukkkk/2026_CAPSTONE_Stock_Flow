package com.stockflow.api.alpaca.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AlpacaTradeMessage {

    @JsonProperty("T")
    private String type;

    @JsonProperty("S")
    private String symbol;

    @JsonProperty("p")
    private Double price;

    @JsonProperty("s")
    private Integer size;

    @JsonProperty("t")
    private String timestamp;

    @JsonProperty("x")
    private String exchange;

    @JsonProperty("c")
    private String[] conditions;
}
