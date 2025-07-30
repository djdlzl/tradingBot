"""
환경별 설정 관리자
"""
import json
import os
from typing import Dict, Any
from pathlib import Path


class EnvironmentConfig:
    """환경별 설정을 관리하는 클래스"""
    
    def __init__(self, environment: str = None):
        """
        환경 설정을 초기화합니다.
        
        Args:
            environment (str): 환경명 ('staging' 또는 'production')
                             None일 경우 TRADING_ENV 환경변수에서 읽어옴
        """
        # 환경 변수에서 환경 설정 읽기 (기본값: staging)
        self.environment = environment or os.getenv('TRADING_ENV', 'staging')
        
        # config 폴더 경로
        self.config_dir = Path(__file__).parent
        
        # 환경별 설정 로드
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """환경별 설정 파일을 로드합니다."""
        config_file = self.config_dir / f"{self.environment}.json"
        
        if not config_file.exists():
            raise FileNotFoundError(f"환경 설정 파일을 찾을 수 없습니다: {config_file}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"환경 설정 파일 파싱 오류: {e}")
    
    def get_tr_id(self, api_name: str) -> str:
        """
        API별 tr_id를 반환합니다.
        
        Args:
            api_name (str): API 이름 (예: 'stock_price', 'order_buy')
            
        Returns:
            str: 해당 API의 tr_id
            
        Raises:
            KeyError: 존재하지 않는 API 이름인 경우
        """
        try:
            return self.config['tr_ids'][api_name]
        except KeyError:
            raise KeyError(f"'{api_name}' API의 tr_id를 찾을 수 없습니다. "
                         f"사용 가능한 API: {list(self.config['tr_ids'].keys())}")
    
    def is_mock_environment(self) -> bool:
        """현재 환경이 모의투자 환경인지 확인합니다."""
        return self.config.get('is_mock', True)
    
    def get_base_url(self) -> str:
        """API 기본 URL을 반환합니다."""
        return self.config['api_endpoints']['base_url']
    
    def get_environment_info(self) -> Dict[str, Any]:
        """현재 환경 정보를 반환합니다."""
        return {
            'environment': self.environment,
            'description': self.config.get('description', ''),
            'is_mock': self.is_mock_environment(),
            'available_apis': list(self.config['tr_ids'].keys())
        }
    
    def list_available_tr_ids(self) -> Dict[str, str]:
        """사용 가능한 모든 tr_id 목록을 반환합니다."""
        return self.config['tr_ids'].copy()


# 전역 환경 설정 인스턴스
env_config = EnvironmentConfig()


def get_tr_id(api_name: str) -> str:
    """편의 함수: tr_id를 가져옵니다."""
    return env_config.get_tr_id(api_name)


def is_mock() -> bool:
    """편의 함수: 모의투자 환경인지 확인합니다."""
    return env_config.is_mock_environment()


def get_current_environment() -> str:
    """편의 함수: 현재 환경명을 반환합니다."""
    return env_config.environment


if __name__ == "__main__":
    # 테스트 코드
    print("=== 환경 설정 테스트 ===")
    print(f"현재 환경: {get_current_environment()}")
    print(f"모의투자 여부: {is_mock()}")
    print(f"환경 정보: {env_config.get_environment_info()}")
    print(f"주가 조회 tr_id: {get_tr_id('stock_price')}")
    print(f"매수 주문 tr_id: {get_tr_id('order_buy')}")
