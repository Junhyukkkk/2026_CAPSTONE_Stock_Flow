package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.core.retry.RetryPolicy;
import com.stockflow.core.retry.RetryService;
import com.stockflow.realtime.dlq.DLQService;
import com.stockflow.realtime.transaction.IdempotencyChannels;
import com.stockflow.realtime.transaction.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * StorageService 단위 테스트
 *
 * "데이터 유실 없음"의 핵심 경로 — 성공 시 커밋 후 멱등성 마킹, 실패 시 DLQ 라우팅을 검증한다.
 */
@ExtendWith(MockitoExtension.class)
class StorageServiceTest {

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private MarketTickBulkWriter marketTickBulkWriter;

    @Mock
    private InstrumentRegistryService instrumentRegistryService;

    @Mock
    private TransactionTemplate transactionTemplate;

    @Mock
    private RetryService retryService;

    @Mock
    private RetryPolicy retryPolicy;

    @Mock
    private ErrorClassifier errorClassifier;

    @Mock
    private DLQService dlqService;

    @Mock
    private PipelineStageMetrics stageMetrics;

    private StorageService storageService;

    private List<NormalizedTradeDTO> trades;

    @BeforeEach
    void setUp() {
        storageService = new StorageService(
                idempotencyService,
                marketTickBulkWriter,
                instrumentRegistryService,
                transactionTemplate,
                retryService,
                retryPolicy,
                errorClassifier,
                dlqService,
                stageMetrics);
        ReflectionTestUtils.setField(storageService, "topicName", "market.normalized");

        long now = System.currentTimeMillis();
        trades = List.of(NormalizedTradeDTO.builder()
                .symbol("BTCUSDT")
                .price(new BigDecimal("50000.00"))
                .volume(new BigDecimal("0.1"))
                .timestamp(now)
                .receivedAt(now)
                .source("BINANCE")
                .tradeId("test-trade-id-1")
                .exchange("BINANCE")
                .marketType("CRYPTO")
                .build());

        // 전부 신규 메시지(멱등성 체크 통과)로 취급
        lenient().when(idempotencyService.areAlreadyProcessed(eq(IdempotencyChannels.STORAGE), anyList()))
                .thenReturn(List.of(false));

        // transactionTemplate.executeWithoutResult() 가 실제로 콜백을 실행하도록 스텁
        lenient().doAnswer(invocation -> {
            Consumer<TransactionStatus> callback = invocation.getArgument(0);
            callback.accept(null);
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        // stageMetrics.time(stage, Runnable) 이 실제로 Runnable을 실행하도록 스텁
        lenient().doAnswer(invocation -> {
            Runnable r = invocation.getArgument(1);
            r.run();
            return null;
        }).when(stageMetrics).time(any(String.class), any(Runnable.class));
    }

    @Test
    void saveBatch_success_marksIdempotencyAfterCommit() {
        boolean result = storageService.saveBatch(trades, "storage-group");

        assertTrue(result);
        verify(marketTickBulkWriter, times(1)).insertBatch(trades);
        verify(instrumentRegistryService, times(1)).registerDistinctFromTrades(trades);
        verify(idempotencyService, times(1)).markBatchAsProcessed(IdempotencyChannels.STORAGE, trades);
        verifyNoInteractions(dlqService);
    }

    @Test
    void saveBatch_nonRetryableFailure_routesToDlqAndReturnsFalse() {
        RuntimeException boom = new RuntimeException("bad data");
        doThrow(boom).when(marketTickBulkWriter).insertBatch(anyList());
        when(errorClassifier.classify(boom)).thenReturn(ErrorType.VALIDATION_ERROR);
        when(errorClassifier.isRetryable(ErrorType.VALIDATION_ERROR)).thenReturn(false);

        boolean result = storageService.saveBatch(trades, "storage-group");

        assertFalse(result);
        verify(dlqService, times(1))
                .sendBatchToDLQ(eq("market.normalized"), eq(trades), eq(boom), eq("storage-group"));
        verify(idempotencyService, never()).markBatchAsProcessed(any(), any());
    }

    @Test
    void saveBatch_retriesExhausted_routesToDlqAndReturnsFalse() throws Exception {
        RuntimeException boom = new RuntimeException("db down");
        doThrow(boom).when(marketTickBulkWriter).insertBatch(anyList());
        when(errorClassifier.classify(boom)).thenReturn(ErrorType.STORAGE_ERROR);
        when(errorClassifier.isRetryable(ErrorType.STORAGE_ERROR)).thenReturn(true);
        when(retryService.executeWithRetry(any(), eq(ErrorType.STORAGE_ERROR), eq(retryPolicy)))
                .thenThrow(boom);

        boolean result = storageService.saveBatch(trades, "storage-group");

        assertFalse(result);
        verify(dlqService, times(1))
                .sendBatchToDLQ(eq("market.normalized"), eq(trades), eq(boom), eq("storage-group"));
        verify(idempotencyService, never()).markBatchAsProcessed(any(), any());
    }
}
