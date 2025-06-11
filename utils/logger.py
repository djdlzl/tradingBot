import os
import json
import logging
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler
from utils.slack_logger import SlackLogger

class TradingLogger:
    """
    트레이딩 시스템을 위한 통합 로깅 시스템
    - 콘솔, 파일, Slack에 동시 로깅
    - 트랜잭션 추적을 위한 고유 ID 생성
    - 구조화된 JSON 로그 포맷
    - 다양한 로그 레벨 지원
    """
    # 로그 레벨 매핑
    LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    def __init__(self, app_name='tradingBot', log_level='INFO', slack_enabled=True, 
                 log_dir='logs', slack_channel='trading-log', session_id=None):
        """
        로깅 시스템 초기화
        
        Args:
            app_name: 애플리케이션 이름
            log_level: 로깅 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            slack_enabled: Slack 로깅 활성화 여부
            log_dir: 로그 파일 저장 디렉토리
            slack_channel: Slack 채널 이름
            session_id: 세션 ID (없으면 자동 생성)
        """
        self.app_name = app_name
        self.session_id = session_id or str(uuid.uuid4())[:8]
        self.slack_enabled = slack_enabled
        self.slack_channel = slack_channel
        self.transactions = {}  # 트랜잭션 추적용
        
        # 로거 설정
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(self.LEVELS.get(log_level.upper(), logging.INFO))
        self.logger.handlers = []  # 핸들러 초기화
        
        # 콘솔 로깅 설정
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._get_formatter())
        self.logger.addHandler(console_handler)
        
        # 파일 로깅 설정
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 일별 로그 파일
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(log_dir, f'{app_name}_{today}.log')
        
        # 로테이팅 파일 핸들러 (10MB 단위로 최대 10개 파일)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=10, encoding='utf-8'
        )
        file_handler.setFormatter(self._get_formatter())
        self.logger.addHandler(file_handler)
        
        # Slack 로거 설정
        if slack_enabled:
            self.slack_logger = SlackLogger(default_channel=slack_channel)
        else:
            self.slack_logger = None
            
        # 시작 로그
        self.info('시스템', f'{app_name} 로깅 시스템이 초기화되었습니다', 
                 {'log_level': log_level, 'session_id': self.session_id})
    
    def _get_formatter(self):
        """로그 포맷터 생성"""
        return logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
    
    def _format_message(self, category, message, data=None):
        """로그 메시지 포맷팅"""
        log_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'session_id': self.session_id,
            'category': category,
            'message': message
        }
        
        if data:
            log_data['data'] = data
            
        return json.dumps(log_data)
    
    def _log(self, level, category, message, data=None, error=None, tx_id=None):
        """내부 로깅 함수"""
        # 트랜잭션 ID 생성 또는 사용
        if tx_id is None and level != 'DEBUG':
            tx_id = str(uuid.uuid4())[:8]
            
        # 데이터에 트랜잭션 ID 추가
        context_data = data or {}
        if tx_id:
            context_data['tx_id'] = tx_id
            
        # 에러 정보 추가
        if error:
            context_data['error'] = str(error)
            if hasattr(error, '__traceback__'):
                import traceback
                context_data['traceback'] = traceback.format_exc()
                
        # 카테고리 표시 포맷
        cat_msg = f"[{category}] {message}"
        
        # 파이썬 로거에 로깅
        log_method = getattr(self.logger, level.lower())
        log_method(cat_msg)
        
        # 파일에 상세 정보 로깅
        detail_msg = self._format_message(category, message, context_data)
        if level != 'DEBUG':  # DEBUG 레벨은 상세 정보 로깅 생략
            self.logger.debug(detail_msg)
        
        # Slack에 로깅 (INFO 이상 레벨만)
        if self.slack_enabled and self.slack_logger and level.upper() != 'DEBUG':
            self.slack_logger.send_log(
                level=level,
                message=f"{category}: {message}",
                error=error,
                context=context_data,
                channel=self.slack_channel
            )
            
        return tx_id
            
    def debug(self, category, message, data=None, tx_id=None):
        """디버그 로깅"""
        return self._log('DEBUG', category, message, data, tx_id=tx_id)
        
    def info(self, category, message, data=None, tx_id=None):
        """정보 로깅"""
        return self._log('INFO', category, message, data, tx_id=tx_id)
        
    def warning(self, category, message, data=None, error=None, tx_id=None):
        """경고 로깅"""
        return self._log('WARNING', category, message, data, error, tx_id)
        
    def error(self, category, message, data=None, error=None, tx_id=None):
        """에러 로깅"""
        return self._log('ERROR', category, message, data, error, tx_id)
        
    def critical(self, category, message, data=None, error=None, tx_id=None):
        """치명적 오류 로깅"""
        return self._log('CRITICAL', category, message, data, error, tx_id)
    
    # 트랜잭션 관리 기능
    def start_transaction(self, category, action, data=None):
        """트랜잭션 시작 로깅"""
        tx_id = str(uuid.uuid4())[:8]
        self.transactions[tx_id] = {
            'start_time': datetime.now(),
            'category': category,
            'action': action,
            'status': 'started',
            'data': data or {}
        }
        
        self.info(category, f"{action} 시작", 
                 {'status': 'started', **self.transactions[tx_id]['data']}, tx_id)
        return tx_id
    
    def end_transaction(self, tx_id, status='completed', result=None, error=None):
        """트랜잭션 종료 로깅"""
        if tx_id not in self.transactions:
            self.warning('트랜잭션', f"알 수 없는 트랜잭션 종료 시도: {tx_id}")
            return False
            
        tx = self.transactions[tx_id]
        end_time = datetime.now()
        duration = (end_time - tx['start_time']).total_seconds()
        
        log_data = {
            'status': status,
            'duration_sec': duration,
            **tx['data']
        }
        
        if result:
            log_data['result'] = result
            
        if status == 'completed':
            self.info(tx['category'], f"{tx['action']} 완료", log_data, tx_id)
        elif status == 'failed':
            self.error(tx['category'], f"{tx['action']} 실패", log_data, error, tx_id)
        else:
            self.warning(tx['category'], f"{tx['action']} {status}", log_data, error, tx_id)
            
        # 트랜잭션 데이터 업데이트
        tx['end_time'] = end_time
        tx['duration'] = duration
        tx['status'] = status
        if result:
            tx['result'] = result
        if error:
            tx['error'] = str(error)
            
        return True
        
    # 세션 모니터링 유틸리티
    def session_update(self, session_state, account_state=None):
        """세션 상태 업데이트 로깅"""
        self.info('세션', f"세션 상태 업데이트", {
            'session_state': session_state,
            'account_state': account_state
        })
    
    # 거래 관련 유틸리티 함수
    def order_attempt(self, ticker, order_type, quantity, price, reason=None):
        """주문 시도 로깅"""
        data = {
            'ticker': ticker,
            'order_type': order_type,
            'quantity': quantity,
            'price': price
        }
        if reason:
            data['reason'] = reason
            
        tx_id = self.start_transaction('거래', f"{order_type} 주문 시도", data)
        return tx_id
        
    def order_result(self, tx_id, success, order_id=None, result=None, error=None):
        """주문 결과 로깅"""
        status = 'completed' if success else 'failed'
        result_data = {'order_id': order_id} if order_id else {}
        if result:
            result_data.update(result)
            
        return self.end_transaction(tx_id, status, result_data, error)
        
    def account_diff(self, expected, actual, description='계좌 불일치 감지'):
        """계좌 데이터 불일치 로깅"""
        # 두 딕셔너리의 차이점 계산
        diffs = {}
        all_keys = set(expected.keys()) | set(actual.keys())
        
        for key in all_keys:
            if key not in expected:
                diffs[key] = {'expected': None, 'actual': actual[key]}
            elif key not in actual:
                diffs[key] = {'expected': expected[key], 'actual': None}
            elif expected[key] != actual[key]:
                diffs[key] = {'expected': expected[key], 'actual': actual[key]}
                
        if diffs:
            self.warning('계좌검증', description, {'differences': diffs})
            return False
        return True


# 싱글톤 인스턴스
_logger_instance = None

def get_logger(app_name='tradingBot', log_level=None, **kwargs):
    """싱글톤 로거 인스턴스 반환"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = TradingLogger(app_name=app_name, log_level=log_level or 'INFO', **kwargs)
    return _logger_instance


# 사용 예시
if __name__ == "__main__":
    # 로거 초기화
    logger = get_logger(log_level='DEBUG')
    
    # 기본 로깅
    logger.info('시스템', '애플리케이션이 시작되었습니다')
    
    # 트랜잭션 로깅
    tx = logger.start_transaction('매수', '삼성전자 매수', {
        'ticker': '005930',
        'quantity': 10,
        'price': 75000
    })
    
    # 트랜잭션 종료
    logger.end_transaction(tx, 'completed', {'order_id': '12345'})
    
    # 에러 로깅
    try:
        1/0
    except Exception as e:
        logger.error('계산', '나누기 오류', {'operands': [1, 0]}, e)
