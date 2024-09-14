from api.kis_api import KISApi
import json

def main():
    kis_api = KISApi()

    kis_api._set_headers(is_mock=False, tr_id="FHKST130000C0")
    # print(json.dumps(kis_api.headers, indent=2))
    # 상한가 종목 조회
    upper_limit_stocks = kis_api.get_upper_limit_stocks()
    if upper_limit_stocks:
        print("Upper Limit Stocks:")
        ul_stocks_list = kis_api.print_korean_response(upper_limit_stocks)
        

    # # 상승/하락 순위 조회
    # updown_rank = kis_api.get_upAndDown_rank()
    # if updown_rank:
    #     print("\nUp and Down Rank:")
    #     kis_api.print_korean_response(updown_rank)

if __name__ == "__main__":
    main()