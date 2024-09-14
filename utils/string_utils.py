"""한글 관련 문자열 유틸리티 함수를 제공하는 모듈입니다."""

def unicode_to_korean(unicode_string):
    """
    유니코드 문자열을 한글로 변환합니다.
    
    :param unicode_string: 변환할 유니코드 문자열
    :return: 변환된 한글 문자열
    """
    try:
        # 유니코드 이스케이프 시퀀스를 바이트로 인코딩한 후 UTF-8로 디코딩
        return unicode_string.encode('utf-8').decode('unicode_escape')
    except UnicodeDecodeError:
        # 디코딩 중 오류가 발생하면 원래 문자열 반환
        return unicode_string