# config/__init__.py

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# [유사도 설정]
# THRESHOLD가 높을수록 필터가 '까다로워져서' 완전히 판박이인 기사들만 골라냄
# TITLE_THRESHOLD: 제목 유사도가 이보다 높으면 같은 사건 그룹으로 묶임
# CONTENT_THRESHOLD: 그룹 내 본문 유사도가 이보다 높으면 최종적으로 '우라까이' 기사로 판정
TITLE_THRESHOLD = float(os.getenv("TITLE_THRESHOLD", 0.15))
CONTENT_THRESHOLD = float(os.getenv("CONTENT_THRESHOLD", 0.4))

# [네이버 API 설정]
NAVER_ID = os.getenv("NAVER_ID")
NAVER_SECRET = os.getenv("NAVER_SECRET")

# [저장소 설정]
BASE_OUTPUT_PATH = "outputs"