# tradingBot (한국투자증권 자동매매 시스템)

## 개요
이 프로젝트는 한국투자증권(KIS) OpenAPI와 웹소켓을 활용하여 자동으로 주식 매매를 수행하는 파이썬 기반 트레이딩 봇입니다. MariaDB를 통한 세션 관리, Slack 실시간 알림, 다양한 매매 전략(상승 눌림목, 상한가 추적 등), 견고한 예외처리 및 로깅 시스템을 갖추고 있습니다.

---

## 주요 기능
- **KIS OpenAPI/웹소켓 연동:** 실시간 시세, 주문, 체결 데이터 수신 및 주문 실행
- **자동 매매 전략:** 상한가 종목 매수, 눌림목 매수, 조건부 매도 등 다양한 전략 구현
- **DB 세션 관리:** MariaDB/MySQL 연동으로 체결/잔고/세션 정합성 유지
- **Slack 알림:** 실시간 매매 내역, 에러, 주요 이벤트 Slack 전송
- **스케줄러:** APScheduler로 장중/장후 자동 실행 및 반복 매매 지원
- **로깅:** 파일 및 콘솔 거래 로그, 에러 및 이벤트 기록
- **환경 변수 기반 보안 설정:** 민감 정보는 .env로 관리

---

## 디렉터리 구조
```
tradingBot/
├── main.py                  # 메인 실행/스케줄 관리
├── api/
│   ├── kis_api.py           # KIS REST API 연동
│   ├── kis_websocket.py     # KIS 웹소켓 실시간 데이터
│   └── krx_api.py           # KRX(한국거래소) 데이터 연동
├── trading/
│   └── trading_upper.py     # 주요 매매 로직 (상승 눌림목 등)
├── database/
│   └── db_manager_upper.py  # DB 세션/잔고/체결 관리
├── config/
│   ├── config.py            # API키, DB, 슬랙 등 환경설정
│   └── condition.py         # 매매 조건/파라미터
├── utils/
│   ├── trading_logger.py    # 거래 로그
│   ├── slack_logger.py      # 슬랙 알림
│   ├── date_utils.py        # 날짜 유틸
│   └── 기타 유틸리티
├── requirements.txt         # 필수 파이썬 패키지
├── .env                     # 환경 변수 (API키 등, git 제외)
└── README.md                # 이 문서
```

---

## 설치 및 실행 방법
1. **패키지 설치**
   ```bash
   pip install -r requirements.txt
   ```
2. **.env 파일 작성**
   - `config/config.py`와 `.env.example` 참고 (API 키, 계좌번호, DB 정보, 슬랙 토큰 등)
3. **MariaDB/MySQL 준비**
   - DB 정보는 환경변수로 지정, 최초 실행 시 테이블 자동 생성
4. **실행**
   ```bash
   python main.py
   ```

---

## 환경 변수 예시 (.env)
```
R_APP_KEY=발급받은_실전_API_KEY
R_APP_SECRET=발급받은_실전_SECRET
M_APP_KEY=발급받은_모의투자_API_KEY
M_APP_SECRET=발급받은_모의투자_SECRET
R_ACCOUNT_NUMBER=12345678-01
M_ACCOUNT_NUMBER=12345678-02
DB_HOST=localhost
DB_USER=root
DB_PASS=비밀번호
DB_NAME=quant_trading
DB_PORT=3306
SLACK_TOKEN=xoxb-...
```

---

## 주요 파일 설명
- **main.py**: 스케줄러 및 전체 매매 프로세스 관리, 장중 자동 실행
- **api/kis_api.py**: KIS REST API 연동(토큰, 주문, 시세 등)
- **api/kis_websocket.py**: 실시간 시세 수신 및 매매 트리거
- **trading/trading_upper.py**: 매매 전략 로직(상승 눌림목, 상한가 추적, 자동 매수/매도)
- **database/db_manager_upper.py**: DB 연결, 세션/잔고/체결 정보 관리
- **utils/trading_logger.py**: 거래 상세 로그 기록
- **utils/slack_logger.py**: 슬랙 알림 전송
- **config/config.py, condition.py**: 환경 변수 및 매매 조건 설정

---

## 참고 및 팁
- **실전/모의투자 전환**: 환경변수 및 config에서 계좌/키만 바꾸면 됨
- **DB 세션 정합성**: 매도 후 잔고/세션 동기화, 중복매도 방지 로직 포함
- **에러 및 예외처리**: 웹소켓, DB, API 통신 등에서 견고한 예외처리 구현
- **Slack 연동**: 주요 이벤트 실시간 알림, 장애 시 빠른 대응 가능

---

## 문의 및 기여
- 코드 개선/문의: Pull Request 또는 Issue 등록
- 본 코드는 개인 연구/백테스트/실전 자동매매용으로 작성됨

---

## License
- MIT License (자유롭게 사용/수정/배포 가능)
