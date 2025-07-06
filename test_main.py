import pytest
from trading.trading_upper import TradingUpper
from zoneinfo import ZoneInfo
KST = ZoneInfo("Asia/Seoul")


def test_fetch_and_save_previous_upper_stocks():
    tu = TradingUpper()
    tu.fetch_and_save_previous_upper_stocks()
    tu.fetch_and_save_previous_upper_limit_stocks()

if __name__ == '__main__':
    test_fetch_and_save_previous_upper_stocks()