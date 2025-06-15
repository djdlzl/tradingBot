"""
거래 로깅을 위한 로거 모듈입니다.
날짜, 시간 정보를 포함하여 거래 행위와 관련된 모든 활동을 로깅합니다.
"""

import os
import logging
from datetime import datetime
import json
from logging.handlers import RotatingFileHandler
from pathlib import Path

class TradingLogger:
    """
    거래 관련 로깅을 수행하는 클래스입니다.
    파일 로깅과 콘솔 출력을 모두 지원합니다.
    """
    
    def __init__(self, log_dir='logs'):
        """
        로거 초기화
        
        Args:
            log_dir: 로그 파일이 저장될 디렉토리
        """
        # 로그 디렉토리 생성
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        
        # 로거 설정
        self.logger = logging.getLogger('trading_logger')
        self.logger.setLevel(logging.DEBUG)
        
        # 이미 핸들러가 설정되어 있는지 확인
        if not self.logger.handlers:
            # 로그 파일명 설정 (날짜별)
            current_date = datetime.now().strftime('%Y-%m-%d')
            log_file = Path(log_dir) / f'trading_log_{current_date}.log'
            
            # 파일 핸들러 설정 (10MB 크기로 최대 10개 파일 유지)
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=10,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            
            # 콘솔 핸들러 설정
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 포맷터 설정
            formatter = logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                '%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            # 핸들러 추가
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def _format_context(self, context):
        """
        컨텍스트 정보를 문자열로 변환합니다.
        
        Args:
            context: 로그 관련 추가 정보를 담은 딕셔너리
            
        Returns:
            str: 포맷팅된 컨텍스트 문자열
        """
        if not context:
            return ""
        
        try:
            return " | " + " | ".join([f"{k}: {v}" for k, v in context.items()])
        except Exception as e:
            return f" | 컨텍스트 포맷팅 실패: {str(e)}"
    
    def debug(self, message, context=None):
        """디버그 레벨 로깅"""
        self.logger.debug(f"{message}{self._format_context(context)}")
    
    def info(self, message, context=None):
        """인포 레벨 로깅"""
        self.logger.info(f"{message}{self._format_context(context)}")
    
    def warning(self, message, context=None):
        """경고 레벨 로깅"""
        self.logger.warning(f"{message}{self._format_context(context)}")
    
    def error(self, message, context=None):
        """에러 레벨 로깅"""
        self.logger.error(f"{message}{self._format_context(context)}")
    
    def critical(self, message, context=None):
        """크리티컬 레벨 로깅"""
        self.logger.critical(f"{message}{self._format_context(context)}")

    def log_action(self, action_type, message, context=None):
        """
        거래 액션 로깅
        
        Args:
            action_type: 액션 타입 (매수, 매도, 세션 등)
            message: 로그 메시지
            context: 추가 정보를 담은 딕셔너리
        """
        formatted_message = f"[{action_type}] {message}"
        self.info(formatted_message, context)
    
    def log_order(self, order_type, ticker, quantity, price=None, order_result=None, context=None):
        """
        주문 관련 로깅
        
        Args:
            order_type: 주문 타입 (매수/매도)
            ticker: 종목 코드
            quantity: 주문 수량
            price: 주문 가격 (옵션)
            order_result: 주문 결과 (옵션)
            context: 추가 정보
        """
        ctx = {
            "종목코드": ticker,
            "수량": quantity
        }
        
        if price is not None:
            ctx["가격"] = price
            
        if order_result:
            ctx["주문번호"] = order_result.get('output', {}).get('ODNO', '없음')
            ctx["상태코드"] = order_result.get('rt_cd', '없음')
            ctx["메시지"] = order_result.get('msg1', '없음')
        
        if context:
            ctx.update(context)
            
        self.log_action(f"{order_type} 주문", f"{order_type} 주문 실행", ctx)
    
    def log_session(self, action, session_id, context=None):
        """
        세션 관련 로깅
        
        Args:
            action: 세션 액션 (생성, 업데이트, 삭제 등)
            session_id: 세션 ID
            context: 추가 정보
        """
        ctx = {"세션ID": session_id}
        if context:
            ctx.update(context)
            
        self.log_action("세션", f"세션 {action}", ctx)
    
    def log_error(self, error_source, exception, context=None):
        """
        예외 상황 로깅
        
        Args:
            error_source: 에러 발생 위치
            exception: 예외 객체
            context: 추가 정보
        """
        ctx = {"에러메시지": str(exception)}
        if context:
            ctx.update(context)
            
        self.error(f"[{error_source}] 에러 발생", ctx)
