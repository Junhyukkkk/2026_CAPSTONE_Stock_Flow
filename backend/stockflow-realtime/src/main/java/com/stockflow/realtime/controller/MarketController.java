package com.stockflow.realtime.controller;

import com.stockflow.realtime.market.MarketOverviewService;
import com.stockflow.realtime.market.dto.MarketOverviewResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
@Tag(name = "Market", description = "시장 개요 API")
public class MarketController {

    private final MarketOverviewService marketOverviewService;

    @GetMapping("/overview")
    @Operation(summary = "시장 개요 조회", description = "상위 등락 종목 목록과 실시간 활성 종목 수를 반환합니다.")
    public MarketOverviewResponse getOverview(
            @Parameter(description = "마켓 타입 필터 (STOCK, CRYPTO). 생략 시 전체.")
            @RequestParam(required = false) String marketType
    ) {
        return marketOverviewService.getOverview(marketType);
    }
}
