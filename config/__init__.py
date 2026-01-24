# config/__init__.py

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# [유사도 설정]
# THRESHOLD가 높을수록 필터가 '까다로워져서' 완전히 판박이인 기사들만 골라냄
# TITLE_THRESHOLD: 제목 유사도가 이보다 높으면 같은 사건 그룹으로 묶임
# CONTENT_THRESHOLD: 그룹 내 본문 유사도가 이보다 높으면 최종적으로 '우라까이' 기사로 판정
TITLE_THRESHOLD = float(os.getenv("TITLE_THRESHOLD", 0.20))
CONTENT_THRESHOLD = float(os.getenv("CONTENT_THRESHOLD", 0.15))

# [네이버 API 설정]
NAVER_ID = os.getenv("NAVER_ID")
NAVER_SECRET = os.getenv("NAVER_SECRET")

# [저장소 설정]
BASE_OUTPUT_PATH = "outputs"

# [수집에서 제외할 제목 단어]
EXCLUDE_WORDS_STR = " 포토, 헤드라인, [사진], [영상], [화보], [그래픽], 톱뉴스"

# 제목에 반드시 검색어가 들어간 경우만 뽑아내려면 True
SEARCH_KEYWORDS = [
    ("정치권", False), #정치권 키워드는 중복 기사가 거의 없이 나옴    
    ("이재명", False), 
    ("청와대", False), 
    ("더불어민주당", False),
    ("국민의힘", False),
    ("조국혁신당", False),   # False
    ("개혁신당", False),     # False
    ("진보당", False),       # False
    ("국회", True),       # True
]

# "정치권" "이재명" "청와대" "더불어민주당" "국민의힘" "조국혁신당" "개혁신당" "진보당"
# -"정치권" -"이재명" -"청와대" -"더불어민주당" -"국민의힘" -"조국혁신당" -"개혁신당" -"진보당"

# 추가가 필요한 검색어 리스트
# "국회" "대통령??"

# 검색어당 수집할 뉴스 개수, 1000개가 MAX
TOTAL_FETCH_COUNT = 30

# 3분에 한 번 하려면 FETCH_PER_HOURS = 1/20
FETCH_PER_HOURS = 3
AGGREGATE_PER_HOURS = 1