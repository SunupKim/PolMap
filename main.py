# main.py
# 단일 키워드 테스트/디버깅용 진입점!!!
# 실행: python main.py

from pipeline import run_news_pipeline
from config import SEARCH_KEYWORDS, TOTAL_FETCH_COUNT
    
if __name__ == "__main__":
    print("단일 키워드 테스트 모드")
    
    for kw, is_required, _ in SEARCH_KEYWORDS:
        try:
            # 파이프라인 함수에 is_required 옵션을 함께 전달
            run_news_pipeline(kw, TOTAL_FETCH_COUNT, is_required)
        except Exception as e:
            print(f"!!! [{kw}] 파이프라인 실행 중 오류 발생: {e}")