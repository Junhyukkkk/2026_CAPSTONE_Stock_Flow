package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class StorageKafkaConsumer {

    // TODO: TradeStorageService 주입 (TimescaleDB Bulk Insert)

    @KafkaListener(
            topics = "market.normalized",
            groupId = "storage-consumer-group",
            containerFactory = "storageKafkaListenerContainerFactory"
    )
    public void onMessage(List<NormalizedTradeDTO> trades, Acknowledgment ack) {
        try {
            log.info("[Storage] Received batch: {} trades", trades.size());

            // 첫 번째, 마지막 데이터 로깅 (디버깅용)
            if (!trades.isEmpty()) {
                NormalizedTradeDTO first = trades.get(0);
                NormalizedTradeDTO last = trades.get(trades.size() - 1);
                log.debug("[Storage] First: {} @ {}, Last: {} @ {}",
                        first.getSymbol(), first.getPrice(),
                        last.getSymbol(), last.getPrice());
            }

            // TODO: TimescaleDB Bulk Insert
            // tradeStorageService.bulkInsert(trades);

            ack.acknowledge();
            log.debug("[Storage] Acknowledged {} trades", trades.size());

        } catch (Exception e) {
            log.error("[Storage] Failed for {} trades: {}",
                    trades.size(), e.getMessage(), e);
            // 실패 시 ack 안 함 → 재처리됨
        }
    }
}
