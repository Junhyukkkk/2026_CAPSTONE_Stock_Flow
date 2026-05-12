package com.stockflow.realtime.stock.dto;

import lombok.Getter;

@Getter
public class InstrumentUpdateRequest {
    private String name;
    private String exchange;
}
