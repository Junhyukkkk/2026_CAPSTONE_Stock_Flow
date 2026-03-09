package com.stockflow.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubscribeRequest {

    private List<String> symbols; // ["AAPL", "TSLA", "BTCUSDT"]
}
