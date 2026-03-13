Kafka + Redis 스트리밍 아키텍처 요약
실시간 주가 수집과 시계열 DB를 활용한 대용량 주가 분석/예측/백테스팅 서비스
권재욱 · 최준혁 | 서울과학기술대학교 ITM전공 캡스톤디자인
1. 전체 데이터 흐름
시스템의 전체 데이터 흐름을 단계별로 정리하면 다음과 같다.

단계	구간	설명
①	외부 API → Python	Binance/Alpaca WebSocket으로 실시간 체결 데이터 수신
②	Python → Kafka	Python Producer가 원본 데이터를 정규화한 후 Kafka로 전송
③	Kafka → Spring Consumer	2개 Consumer Group이 동시 수신 (저장용 + 실시간용)
④	Consumer → TimescaleDB	저장용 Consumer가 Bulk Insert로 DB에 영구 저장
⑤	Consumer → Redis	실시간용 Consumer가 Redis에 최신가 캐싱 + Pub/Sub 발행
⑥	Redis → WebSocket → Client	Pub/Sub 메시지를 받아 STOMP WebSocket으로 클라이언트에 Push

⚠ Kafka는 메시지 브로커 역할만 수행한다. 데이터 웨어하우스로 사용하지 않으며, Consumer가 수신 즉시 DB/Redis에 저장한다.
💡 Redis도 영구 저장소가 아니다. TTL로 자동 만료되며, 영구 저장은 오직 TimescaleDB가 담당한다.
 
2. Kafka Topic 설계
2.1 토픽 목록
Topic Name	역할	Partitions	Retention
market.raw.crypto	Binance 원본 JSON 보존 (디버깅용)	6	2시간
market.raw.stock	Alpaca 원본 JSON 보존 (디버깅용)	6	2시간
market.normalized	★ 핵심 토픽: 정규화된 통합 데이터 (모든 Consumer가 구독)	12	4시간
market.dlq	처리 실패 메시지 보관 (Dead Letter Queue)	3	7일

2.2 raw vs normalized 관계
핵심 포인트: Kafka 내부에서 raw → normalized로 변환하는 것이 아니라, Python Producer가 동시에 2곳으로 전송한다:

Python Producer 내부 처리 흐름:

  Binance WebSocket 수신
       │
       ├─── ① 원본 JSON 그대로 → market.raw.crypto (보험용)
       │
       └─── ② 정규화 변환 → market.normalized  (실제 사용)

  Spring Consumer들은 market.normalized만 구독한다.
  raw 토픽은 문제 발생 시 원본 데이터 확인용으로만 사용.
💡 raw 토픽을 Kafka Streams로 변환하는 방식도 있지만, Python에서 이미 데이터를 파싱하고 있으므로 거기서 바로 정규화하는 것이 불필요한 홈을 줄이고 레이턴시도 낮춘다.
2.3 파티셔닝 전략
모든 토픽의 파티션 키는 종목 심볼(symbol)을 사용한다.
이유: 동일 종목의 메시지가 항상 같은 파티션에 들어가므로 순서가 보장되고, Consumer별 병렬 처리도 가능하다.
예시: AAPL 데이터 → 항상 Partition 3으로
      BTCUSDT 데이터 → 항상 Partition 7으로

      → AAPL 데이터는 시간순 보장
      → Consumer 3개가 각각 4개 파티션을 분담하여 병렬 처리
 
3. 데이터 정규화 (Normalization)
Binance와 Alpaca는 완전히 다른 JSON 포맷을 사용한다. 이를 통일된 스키마로 변환하여 하나의 Consumer로 모든 소스를 처리할 수 있게 한다.
3.1 원본 데이터 비교
Binance와 Alpaca가 보내는 실제 데이터 포맷:
필드	Binance (암호화폐)	Alpaca (미국 주식)
심볼	s: "BTCUSDT"	S: "AAPL"
가격	p: "67234.50" (문자열)	p: 178.25 (숫자)
수량	q: "0.015" (문자열)	s: 150 (정수)
시간	T: 1708234567890 (epoch ms)	t: "2025-03-15T14:30:00Z" (ISO)
거래소	없음 (Binance 고정)	x: "IEX" (거래소 명)
⚠ 필드명, 타입, 날짜 포맷이 모두 다르다. 정규화 없이는 Consumer가 API별 분기 처리해야 하므로 복잡도가 급증한다.
3.2 정규화 변환 규칙
Python Producer에서 다음 변환을 수행하여 통일된 스키마로 묶는다:
변환 항목	Binance → 통일	Alpaca → 통일
가격	float("67234.50") → 67234.50	178.25 → 178.25 (변환 불필요)
수량	float("0.015") → 0.015	150 → 150 (변환 불필요)
타임스탬프	1708234567890 → 그대로 (epoch ms)	ISO 8601 → epoch ms 변환
거래소	"BINANCE" 고정값 추가	msg["x"] → "IEX", "NYSE" 등
시장 구분	"CRYPTO" 고정	"STOCK" 고정
3.3 정규화 결과 예시 (market.normalized 토픽에 전송되는 실제 JSON)
// Binance 캔봄화폐 거래 → 정규화 결과
{
  "source": "BINANCE",
  "symbol": "BTCUSDT",
  "price": 67234.50,
  "volume": 0.015,
  "tradeId": "1234567",
  "exchange": "BINANCE",
  "timestamp": 1708234567890,
  "receivedAt": 1708234567895,
  "marketType": "CRYPTO"
}

// Alpaca 미국주식 거래 → 정규화 결과
{
  "source": "ALPACA",
  "symbol": "AAPL",
  "price": 178.25,
  "volume": 150,
  "tradeId": "987654",
  "exchange": "IEX",
  "timestamp": 1708234560123,
  "receivedAt": 1708234560200,
  "marketType": "STOCK"
}
💡 소스가 다르지만 결과 포맷은 완전히 동일하다. Consumer는 source 필드로 구분할 수 있지만, 처리 로직은 하나로 통일된다.
 
4. Spring Consumer DTO
Spring Kafka Consumer가 market.normalized 토픽의 JSON을 받을 때 사용하는 DTO 클래스:
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class NormalizedTradeDTO {

    @NotBlank
    private String source;       // "BINANCE" | "ALPACA"

    @NotBlank
    private String symbol;        // "AAPL", "BTCUSDT"

    @NotNull
    private BigDecimal price;     // 체결 가격

    @NotNull
    private BigDecimal volume;    // 체결 수량

    @NotBlank
    private String tradeId;       // 원본 거래 ID

    @NotBlank
    private String exchange;      // 거래소 (BINANCE, IEX, NYSE)

    @NotNull
    private Long timestamp;       // 체결 시간 (epoch ms)

    @NotNull
    private Long receivedAt;      // Producer 수신 시각 (epoch ms)

    @NotBlank
    private String marketType;    // "CRYPTO" | "STOCK"
}

필드	타입	용도
source	String	어느 API에서 온 데이터인지 구분
symbol	String	종목 식별자. Kafka 파티션 Key로도 사용
price	BigDecimal	체결 가격. 부동소수점 오차 방지를 위해 BigDecimal 사용
volume	BigDecimal	체결 수량. 암호화폐는 소수점, 주식은 정수
tradeId	String	원본 거래 ID. source + tradeId로 중복 검사
exchange	String	거래소 명. 미국 주식은 IEX, NYSE 등 다양
timestamp	Long	체결 시간 (UTC epoch ms). 모든 소스 동일 포맷
receivedAt	Long	Producer가 수신한 시간. End-to-End 레이턴시 측정용
marketType	String	시장 구분 (CRYPTO/STOCK). 프론트엔드 UI 분류에 활용
⚠ price를 double이 아닌 BigDecimal을 쓴 이유: 금융 데이터에서 float/double은 부동소수점 오차가 발생한다. 0.1 + 0.2 ≠ 0.3 같은 문제가 생길 수 있어 금융 시스템에서는 BigDecimal이 필수이다.
5. Consumer Group 분리
Kafka의 Consumer Group 기능을 활용하여 하나의 토픽을 2개 그룹이 동시에 구독한다:
Consumer Group	역할	처리 방식
storage-consumer-group	DB 저장 전담	최대 500건 묶어서 TimescaleDB Bulk Insert. DB 저장 성공 후 offset 커밋.
realtime-consumer-group	실시간 Push 전담	건건이 즉시 Redis에 캐싱 + Pub/Sub 발행. 실시간성 우선.

동일 메시지가 2개 그룹에게 동시 전달되는 구조:

  market.normalized (파티션 12개)
       │
       ├── storage-consumer-group (3개 인스턴스)
       │     └── 각 4개 파티션 담당 → TimescaleDB
       │
       └── realtime-consumer-group (3개 인스턴스)
             └── 각 4개 파티션 담당 → Redis + WebSocket
💡 Consumer Group 덕분에 DB 저장과 실시간 Push가 서로 영향을 주지 않는다. DB가 느려도 WebSocket Push는 정상 동작한다.
 
6. Redis 캐싱
Realtime Consumer가 Kafka에서 받은 데이터를 Redis에 캐싱한다. REST API 요청 시 DB를 조회하지 않고 Redis에서 즉시 반환한다.
6.1 캐시 Key 설계
Key	Type	TTL	용도
price:latest:AAPL	String (JSON)	60초	종목별 최신 체결 데이터 + 등락률
price:prev-close:AAPL	String	24시간	전일 종가 (등락률 계산용)
6.2 캐싱 처리 흐름
Kafka에서 메시지를 받을 때마다 다음을 수행한다:

① NormalizedTradeDTO 수신 (symbol: AAPL, price: 178.25)

② Redis에서 전일 종가 조회
   GET price:prev-close:AAPL → 176.10

③ 등락률 계산
   change = 178.25 - 176.10 = +2.15
   changePercent = (2.15 / 176.10) * 100 = +1.22%

④ Redis에 최신가 캐싱 (TTL 60초)
   SET price:latest:AAPL {
     "symbol": "AAPL",
     "price": 178.25,
     "change": 2.15,
     "changePercent": 1.22,
     "timestamp": 1708234560123
   }

⑤ REST API에서 /api/price/AAPL 요청 시
   → DB 안 가고 Redis에서 즉시 반환 (< 1ms)
7. Redis Pub/Sub 실시간 브로드캐스트
캐싱과 동시에 Pub/Sub로 메시지를 발행하여 WebSocket 클라이언트에게 실시간 Push한다.
7.1 Pub/Sub 채널
채널 패턴	설명
price:AAPL	종목별 실시간 가격 밌 등락률 브로드캐스트
7.2 전체 처리 흐름
Kafka Consumer                           Client (브라우저)
     │                                       │
     │  PUBLISH price:AAPL "{...}"          │
     └────────▶ Redis ◀───────────────┘
                  │          SUBSCRIBE price:AAPL
                  │
                  ▼
          MessageListener
                  │
                  │  convertAndSend(
                  │    "/topic/price/AAPL", data)
                  ▼
          STOMP WebSocket
                  │
                  ▼
          Client에서 실시간 가격 표시

단계별 역할:

Kafka Consumer: 메시지 수신 → Redis SET (캐싱) + PUBLISH (브로드캐스트) 동시 수행
Redis: 채널에 구독 중인 MessageListener에게 메시지 전달
MessageListener: Redis에서 받은 메시지를 SimpMessagingTemplate로 WebSocket에 전달
Client: STOMP /topic/price/AAPL 구독 → 실시간 가격 업데이트

⚠ Redis Pub/Sub은 메시지를 버퍼링하지 않는다. 구독자가 없으면 메시지는 유실된다.
💡 이는 문제없다 — 새 클라이언트가 연결하면 Redis 캐시(price:latest:{symbol})에서 최신가를 즉시 반환하고, 이후부터 Pub/Sub으로 실시간 업데이트를 받는다.
 
8. 이 구조를 선택한 이유
8.1 왜 Kafka를 쓰는가?
문제	Kafka로 해결
데이터 유실 방지	Producer와 Consumer 사이 버퍼. Consumer가 잠시 죽어도 메시지 보관. 복구 후 이어서 처리.
수집과 처리 분리	Python 수집기와 Spring 처리기가 독립적. 한쪽이 느려도 다른 쪽에 영향 없음.
병렬 처리	파티션 분배로 Consumer 인스턴스 수만 늘리면 처리량 확장 가능.
다중 소비	Consumer Group으로 동일 데이터를 DB 저장 + 실시간 Push 동시 처리.
8.2 왜 Redis를 쓰는가?
문제	Redis로 해결
최신가 빠른 조회	DB 조회 = 수십 ms. Redis 조회 = < 1ms. API 요청 시 즉시 반환.
실시간 브로드캐스트	Pub/Sub로 구독 중인 모든 클라이언트에게 동시 전달. 폴링 불필요.
DB 부하 감소	최신가 조회가 모두 Redis로 처리되므로 DB에는 쓰기만 발생.
자동 만료	TTL로 오래된 데이터 자동 삭제. 메모리 관리 부담 없음.
8.3 Kafka vs Redis 역할 구분
구분	Kafka	Redis
역할	메시지 버퍼링 및 라우팅	캐싱 및 실시간 Pub/Sub
데이터 보관	예 (retention 기간 동안)	아니오 (TTL 만료 시 삭제)
신뢰성	높음 (offset 관리, ACK)	낮음 (Pub/Sub 메시지 유실 가능)
속도	빠름 (~ms)	매우 빠름 (~μs)
용도	수집 ↔ 처리 사이 안전한 전달	클라이언트에게 빠른 전달
💡 정리: Kafka는 "안전하게 전달"하는 역할, Redis는 "빠르게 전달"하는 역할. 둘은 경쟁 관계가 아니라 보완 관계이다.
