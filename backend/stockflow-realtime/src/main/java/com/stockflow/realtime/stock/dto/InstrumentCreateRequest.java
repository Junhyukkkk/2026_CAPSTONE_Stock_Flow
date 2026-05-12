package com.stockflow.realtime.stock.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;

@Getter
public class InstrumentCreateRequest {

    @NotBlank
    private String symbol;

    @NotBlank
    @Pattern(regexp = "CRYPTO|STOCK", message = "marketType must be CRYPTO or STOCK")
    private String marketType;

    @NotBlank
    private String exchange;

    private String name;
}
