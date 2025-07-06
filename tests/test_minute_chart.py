"""
일별분봉조회 테스트 모듈

한국투자증권 API를 사용하여 특정 종목의 일별 분봉 데이터를 조회하는 기능을 테스트합니다.
"""
import json
import requests
import pandas as pd
import sys
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

# 상위 디렉토리를 import path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.kis_api import KISApi

KST = ZoneInfo("Asia/Seoul")


class MinuteChartTest:
    """일별 분봉 조회 테스트를 위한 클래스"""
    
    @staticmethod
    def get_minute_chart(ticker, base_date):
        """
        특정 일자의 분봉 데이터를 조회합니다.
        
        Args:
            ticker (str): 종목 코드 (예: '217270')
            base_date (str): 기준 일자 (YYYYMMDD 형식)
            
        Returns:
            dict: 분봉 데이터
        """
        # KIS API 인스턴스 생성
        kis_api = KISApi()
        
        # 먼저 헤더 설정 (실제 거래용)
        kis_api._set_headers(is_mock=False, tr_id="FHKST03010200")
        
        # 헤더 복사
        headers = kis_api.headers.copy()
        
        # API 엔드포인트
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
        
        # 요청 파라미터
        params = {
            "FID_ETC_CLS_CODE": "",       # 기타분류코드
            "FID_COND_MRKT_DIV_CODE": "J", # 시장분류코드 J:주식, ETF, ETN
            "FID_INPUT_ISCD": ticker,       # 종목코드
            "FID_INPUT_HOUR_1": "130000",  # 조회일자 (YYYYMMDD)
            "FID_INPUT_DATE_1": base_date,  # 조회일자 (YYYYMMDD)
            "FID_HOUR_CLS_CODE": "1",      # 시간분류코드(1:분, D:일, W:주, M:월, Y:년)
            "FID_PW_DATA_INCU_YN": "Y"     # 데이터 누적여부 (Y:누적, N:미누적)
        }
        
        # API 요청
        try:
            response = requests.get(url=url, params=params, headers=headers, timeout=10)
            response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
            result = response.json()
            print(json.dumps(response.json(), indent=2, ensure_ascii=False))
            return result
        except requests.exceptions.RequestException as e:
            print(f"API 요청 중 오류 발생: {e}")
            return None

    @staticmethod
    def get_minute_data(ticker, base_date):
        """
        특정 일자의 분봉 데이터를 조회하고 DataFrame으로 변환합니다.
        
        Args:
            ticker (str): 종목 코드 (예: '217270')
            base_date (str): 기준 일자 (YYYYMMDD 형식)
            
        Returns:
            pd.DataFrame: 분봉 데이터 DataFrame
        """
        result = MinuteChartTest.get_minute_chart(ticker, base_date)
        
        if not result or 'output2' not in result:
            print("데이터를 가져오는데 실패했거나 분봉 데이터가 없습니다.")
            return None
        
        # DataFrame으로 변환
        chart_data = result.get('output2', [])
        df = pd.DataFrame(chart_data)
        
        # 필요한 열만 선택하고 이름 변경
        if not df.empty:
            selected_columns = {
                'stck_cntg_hour': '시간',
                'stck_prpr': '현재가',
                'stck_oprc': '시가',
                'stck_hgpr': '고가',
                'stck_lwpr': '저가',
                'cntg_vol': '거래량',
                'acml_tr_pbmn': '거래대금'
            }
            
            # 존재하는 열만 선택
            existing_columns = {k: v for k, v in selected_columns.items() if k in df.columns}
            df = df[list(existing_columns.keys())]
            df = df.rename(columns=existing_columns)
            
            # 데이터 형식 변환
            numeric_columns = ['현재가', '시가', '고가', '저가', '거래량', '거래대금']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 시간 형식 변환 (HHMM -> HH:MM)
            if '시간' in df.columns:
                df['시간'] = df['시간'].apply(lambda x: f"{x[:2]}:{x[2:]}" if len(x) >= 4 else x)
                
            # 시간 기준으로 정렬
            df = df.sort_values(by='시간')
            
            return df
        else:
            print("분봉 데이터가 없습니다.")
            return None


def run_test():
    """테스트 실행 함수"""
    print("===== 일별분봉조회 테스트 시작 =====")
    
    # 7월 3일 넵튠(217270) 종목의 하루 분봉 조회
    ticker = "217270"  # 넵튠 종목코드
    base_date = "20250703"  # 7월 3일
    
    print(f"종목코드: {ticker} (넵튠)")
    print(f"조회일자: {base_date}")
    
    # 원시 API 응답 조회
    raw_data = MinuteChartTest.get_minute_chart(ticker, base_date)
    if raw_data:
        print("\n[API 응답 헤더]")
        if 'output1' in raw_data:
            for key, value in raw_data['output1'].items():
                print(f"{key}: {value}")
    
    # 분봉 데이터 DataFrame 조회
    df = MinuteChartTest.get_minute_data(ticker, base_date)
    if df is not None:
        print("\n[분봉 데이터]")
        print(df)
        
        # 요약 통계 출력
        print("\n[데이터 요약]")
        print(f"총 데이터 수: {len(df)}행")
        if '시간' in df.columns:
            print(f"거래 시간대: {df['시간'].iloc[0]} ~ {df['시간'].iloc[-1]}")
        if '현재가' in df.columns:
            print(f"종가: {df['현재가'].iloc[-1]:,}원")
            print(f"최고가: {df['고가'].max():,}원")
            print(f"최저가: {df['저가'].min():,}원")
        if '거래량' in df.columns:
            print(f"총 거래량: {df['거래량'].sum():,}주")
        if '거래대금' in df.columns:
            print(f"총 거래대금: {df['거래대금'].sum()/1000000:,.2f}백만원")
    
    print("\n===== 일별분봉조회 테스트 완료 =====")


if __name__ == "__main__":
    run_test()
