# config/__init__.py

import os
from dotenv import load_dotenv

from google import genai 
from google.genai import types

# .env 파일 로드
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")  # Gemini API KEY
gen_client = genai.Client(api_key=GEMINI_API_KEY)
GEMINI_MODEL_2_5 = "gemini-2.5-flash"
NORMAL_TEMPERATURE = 0.2

with open("prompts/system_normal.txt", "r", encoding="utf-8") as f:
    system_instruction_normal = f.read().strip()

GEMINI_CONFIG_NORMAL = types.GenerateContentConfig(    
    system_instruction=system_instruction_normal
)

# [유사도 설정]
# THRESHOLD가 높을수록 필터가 '까다로워져서' 완전히 판박이인 기사들만 골라냄
# TITLE_THRESHOLD: 제목 유사도가 이보다 높으면 같은 사건 그룹으로 묶임
# CONTENT_THRESHOLD: 그룹 내 본문 유사도가 이보다 높으면 최종적으로 '우라까이' 기사로 판정

SINGLE_TITLE_THRESHOLD = 0.20
SINGLE_CONTENT_THRESHOLD = 0.20 #0.15는 너무 낮다

GLOBAL_TITLE_THRESHOLD = 0.10
GLOBAL_CONTENT_THRESHOLD = 0.15

PROBE_TITLE_THRESHOLD = 0.20
PROBE_CONTENT_THRESHOLD = 0.20

# [네이버 API 설정]
NAVER_ID = os.getenv("NAVER_ID")
NAVER_SECRET = os.getenv("NAVER_SECRET")

# [저장소 설정]
BASE_OUTPUT_PATH = "outputs"

# [수집에서 제외할 제목 단어]
EXCLUDE_WORDS_STR = " 포토, 헤드라인, [사진], [영상], [화보], [그래픽], 톱뉴스, [오늘의 주요일정], [투데이 라인업]"

# 수집 주기
AGGREGATE_PER_HOURS = 3

# 파일 경로 지정
OUTPUT_ROOT = "outputs/"
CANONICAL_ARCHIVE_PATH = os.path.join(OUTPUT_ROOT, "aggregated/canonical_archive.csv")

# FETCH_PER_HOURS = 1 이상일 때만 의미 있음, 1보다 작으면 안됨.
# IS_SAMPLE_RUN에 따라 SEARCH_KEYWORDS, TOTAL_FETCH_COUNT가 달라지므로 
# scheduler.py, aggregator.py 모두에 영향을 미친다

#IS_SAMPLE_RUN = True #테스트모드
IS_SAMPLE_RUN = False #실전모드

if IS_SAMPLE_RUN:
    SEARCH_KEYWORDS = [    
        ("이재명", False, 0.0001), # 제목에 반드시 검색어가 들어간 경우만 뽑아내려면 True
        ("청와대", False, 0.0001), 
    ]
    TOTAL_FETCH_COUNT = 30
else:
    SEARCH_KEYWORDS = [    
        
        # 1그룹 : 현재는 1시간에 200건 정도 나온다 -> 2이하 필수다
        ("이재명", False, 1), 
        ("청와대", False, 1), 
        ("더불어민주당", False, 1),
        ("국민의힘", False, 1),
        ("국회", True, 1),       # **True**

        # 2그룹 : 현재는 1시간에 50건 정도 나오고 있음 -> 5이하 필수다
        ("정치권", False, 5), #정치권 키워드는 중복 기사가 거의 없이 나옴    
        ("개혁신당", False, 5),     # False
        ("조국혁신당", False, 5),   # False    

        # 3그룹 : 
        ("진보당", False, 10),       # False로 하면 많게는 하루 250건도 나온다(여론조사) True로는 하루 30건 정도 -> 20 정도도 가능할 듯.
        ("기본소득당", False, 10),
    ]
    TOTAL_FETCH_COUNT = 1000     # 검색어당 수집할 뉴스 개수, 1000개가 MAX        

# 표준 컬럼 순서 (필요시 외부 모듈에서 import해서 사용)
COLUMN_ORDER = [
    "search_keyword", "news_id", "pubDate", "collected_at", 
    #"title_id", "body_id", "is_canon", "replaced_by",
    "title", "description", "link", "originallink", "content"
]

RAW_COLUMNS = [
    "search_keyword", "news_id", "pubDate", "collected_at", 
#    "title_id", "body_id", "is_canon", "replaced_by",
    "title", "description", "link", "originallink", "content"
]

CANONICAL_COLUMNS = [
    "search_keyword", "news_id", "pubDate", "collected_at", 
#    "title_id", "body_id", "is_canon", "replaced_by",
    "title", "description", "link", "originallink", "content",
    # 필요시 추가 컬럼
]


