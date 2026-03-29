package com.stockflow.realtime.controller;

import com.stockflow.realtime.storage.MarketTickPreviewService;
import com.stockflow.realtime.storage.dto.StorageOverviewResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Timescale 저장 경로 데모·점검용 API (읽기 전용).
 */
@RestController
@RequestMapping("/api/storage")
@RequiredArgsConstructor
public class StorageOverviewController {

    private final MarketTickPreviewService marketTickPreviewService;

    @GetMapping("/overview")
    public ResponseEntity<StorageOverviewResponse> overview(
            @RequestParam(required = false) Integer ticksLimit) {
        return ResponseEntity.ok(marketTickPreviewService.overview(ticksLimit));
    }
}
