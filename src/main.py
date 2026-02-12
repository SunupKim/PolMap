# main.py
# 단일 키워드 테스트/디버깅용 진입점
# 실행: python main.py

import os
from datetime import datetime
from pipeline import run_news_pipeline
from config import SEARCH_KEYWORDS
    
if __name__ == "__main__":
    print("단일 키워드 테스트 모드")
    
    # 테스트 실행 시에도 시간대별 로그 폴더 생성
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    current_log_dir = os.path.join("logs", f"test_{run_timestamp}")

    for kw, is_required, fetch_count in SEARCH_KEYWORDS:
        try:
            # 파이프라인 함수에 개별 fetch_count 전달
            run_news_pipeline(kw, fetch_count, is_required, log_dir=current_log_dir)
        except Exception as e:
            print(f"!!! [{kw}] 파이프라인 실행 중 오류 발생: {e}")