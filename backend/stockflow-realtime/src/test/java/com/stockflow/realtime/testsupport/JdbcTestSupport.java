package com.stockflow.realtime.testsupport;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JdbcTemplate 을 목으로 대체하는 리포지토리 테스트용 도우미.
 * Postgres 전용 SQL(jsonb, DISTINCT ON …)은 H2 로 실행할 수 없으므로,
 * 컬럼 이름 → 값 맵으로 ResultSet 을 흉내 내 RowMapper 의 매핑 로직을 검증한다.
 */
public final class JdbcTestSupport {

    private JdbcTestSupport() {
    }

    /** 컬럼 라벨 → 값 맵으로 동작하는 ResultSet 목. 값이 없는 컬럼은 null/0 을 돌려준다. */
    public static ResultSet resultSet(Map<String, Object> columns) {
        AtomicBoolean hasNext = new AtomicBoolean(true);
        Answer<Object> answer = (InvocationOnMock inv) -> {
            String method = inv.getMethod().getName();
            Object[] args = inv.getArguments();
            if ("next".equals(method)) {
                return hasNext.getAndSet(false);
            }
            if (args.length != 1 || !(args[0] instanceof String column)) {
                return Mockito.RETURNS_DEFAULTS.answer(inv);
            }
            Object value = columns.get(column);
            return switch (method) {
                case "getLong" -> value == null ? 0L : ((Number) value).longValue();
                case "getInt" -> value == null ? 0 : ((Number) value).intValue();
                case "getDouble" -> value == null ? 0d : ((Number) value).doubleValue();
                case "getString" -> value == null ? null : value.toString();
                case "getBigDecimal" -> value == null ? null : new BigDecimal(value.toString());
                case "getBoolean" -> value != null && (Boolean) value;
                case "getDate" -> value == null ? null : Date.valueOf((LocalDate) value);
                case "getTimestamp" -> value == null ? null : Timestamp.from((Instant) value);
                case "getObject" -> value;
                default -> Mockito.RETURNS_DEFAULTS.answer(inv);
            };
        };
        return Mockito.mock(ResultSet.class, answer);
    }

    /** next() 가 즉시 false 인 빈 ResultSet. */
    public static ResultSet emptyResultSet() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        try {
            Mockito.when(rs.next()).thenReturn(false);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
        return rs;
    }

    /** 각 행마다 mapper 를 적용한 결과 리스트. */
    public static <T> List<T> mapRows(RowMapper<T> mapper, List<Map<String, Object>> rows) throws Exception {
        List<T> out = new ArrayList<>();
        int i = 0;
        for (Map<String, Object> row : rows) {
            out.add(mapper.mapRow(resultSet(row), i++));
        }
        return out;
    }
}
