# 브랜치 관리 전략 가이드

## 📋 브랜치 구조

```
main (production - 실제계좌)
├─ staging (모의투자)
└─ develop (개발용)
```

## 🔄 워크플로우

### 1. 개발 프로세스
```bash
develop → staging → main
```

### 2. 브랜치별 환경 설정

| 브랜치 | 환경 | TRADING_ENV | 용도 |
|--------|------|-------------|------|
| `develop` | staging | staging | 개발 및 테스트 |
| `staging` | staging | staging | 모의투자 검증 |
| `main` | production | production | 실제계좌 운영 |

### 3. 배포 단계

#### Step 1: 개발 (develop 브랜치)
```bash
git checkout develop
# 새로운 기능 개발
$env:TRADING_ENV = "staging"
python main.py  # 모의투자로 테스트
```

#### Step 2: 스테이징 (staging 브랜치)
```bash
git checkout staging
git merge develop
$env:TRADING_ENV = "staging"
python main.py  # 최종 모의투자 검증
```

#### Step 3: 프로덕션 (main 브랜치)
```bash
git checkout main
git merge staging
$env:TRADING_ENV = "production"
python main.py  # 실제계좌 운영
```

## 🛡️ 안전 장치

### 1. 환경별 자동 분기
- `staging.json`: 모의투자 API 사용
- `production.json`: 실제계좌 API 사용

### 2. 코드 레벨 안전장치
```python
# 환경 확인
if env_config.is_mock_environment():
    print("⚠️  모의투자 환경에서 실행 중")
else:
    print("🚨 실제계좌 환경에서 실행 중 - 주의!")
```

### 3. 배포 전 체크리스트
- [ ] `staging` 브랜치에서 충분한 테스트 완료
- [ ] 환경 설정 파일 확인
- [ ] 로그 및 모니터링 설정 확인
- [ ] 백업 및 롤백 계획 수립

## 🔧 환경 설정 파일 관리

### staging.json (모의투자)
```json
{
  "environment": "staging",
  "is_mock": true,
  "tr_ids": {
    "order_buy": "VTTC0802U",    // V로 시작하는 모의투자 코드
    "order_sell": "VTTC0801U"
  }
}
```

### production.json (실제계좌)
```json
{
  "environment": "production", 
  "is_mock": false,
  "tr_ids": {
    "order_buy": "TTTC0802U",    // T로 시작하는 실거래 코드
    "order_sell": "TTTC0801U"
  }
}
```

## 📝 주의사항

1. **main 브랜치는 항상 안정적이어야 함**
2. **실제계좌 배포 전 반드시 staging에서 검증**
3. **환경 변수 설정 확인 필수**
4. **API 키와 계좌번호는 .env 파일에서 관리**
