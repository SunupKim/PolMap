# PolMap2 프로젝트 가이드 (Copilot 지침)
**사용자가 명시적으로 요청하지 않은 코드 수정이나 개선 제안을 먼저 하지 마시오.**

## 프로젝트 개요
PolMap2는 다음 기능을 수행하는 정치 뉴스 수집 및 중복 제거 시스템이다.
1. 네이버 뉴스 API를 사용해 여러 키워드로 한국 정치 뉴스를 수집한다.
2. 기사 제목과 본문 유사도를 기준으로 필터링, 스크래핑, 클러스터링을 수행한다.
3. 각 클러스터마다 대표 기사(canonical article)를 선정한다.
4. 키워드 단위 결과를 전역으로 통합해 중복을 제거한다.
5. 실행 지표를 기록하고 키워드별 아카이브를 유지한다.

**핵심 통찰**
이 시스템은 뉴스 양보다 데이터 품질을 우선한다. 중복 제거와 대표 기사 선정을 최우선으로 하여, 중복 없는 정제된 뉴스 데이터셋을 제공하는 것이 목적이다.

---

## 아키텍처 및 데이터 흐름

### 다단계 파이프라인(총 12단계)
각 키워드는 독립적으로 이 파이프라인을 거친 뒤, 최종 단계에서 전역으로 병합된다.

1. Fetch 단계
구성 요소: NaverNewsClient
입력: 키워드
출력: API 원시 아이템
목적: 네이버 뉴스 API 호출

2. Store 단계
구성 요소: NewsRepository
입력: 원시 아이템
출력: 신규 기사만 저장
목적: 증분 저장, 링크 기반 중복 제거

3. Pre-Filter 단계
구성 요소: NewsFilter.apply_pre_filter()
입력: 원시 기사
출력: 정제된 기사
목적: 제목 패턴, 스니펫 길이, 동일 제목 제거

4. Scrape 단계
구성 요소: NewsScraper
입력: 기사
출력: 전체 본문
목적: BeautifulSoup 기반 본문 추출

5. Post-Filter 단계
구성 요소: NewsFilter.apply_post_filter()
입력: 전체 기사
출력: 품질 기준을 통과한 기사
목적: 본문 없음, 길이, 발화체 기사 제거

6. Cluster 단계
구성 요소: NewsCluster
입력: 필터링된 기사
출력: 그룹화 결과 및 대표 기사
목적: 제목과 본문 유사도 기준 클러스터링(OR 조건)

7. Deduplicate 단계
구성 요소: aggregator.py
입력: 모든 키워드 아카이브
출력: 전역 아카이브
목적: 링크 기반 전역 중복 제거 및 메타 매핑

## 중요 개념
cluster_id는 유사한 기사 그룹을 의미한다. 그러나 is_canonical=True인 기사만 최종 출력에 남는다. 나머지 기사는 키워드 단위에서는 replaced_by, 전역 단위에서는 global_replaced_by로 대체 관계가 기록된다.

---

## 주요 파일과 역할

### 진입점

**메인 진입점 (스케줄러 - 자동 실행)**
- `scripts/scheduler.py` - 정기 수집 스케줄러 (무한 루프)
  - 실행: `python scripts/scheduler.py` 또는 `python -m scripts.scheduler`
  - 역할: 키워드별 실행 주기 관리, 파이프라인 호출, 실행 로그 기록
  - 출력: `outputs/execution_log.csv` (누적 통계)

**통합 스크립트 (수동 실행)**
- `scripts/aggregator.py` - 전역 뉴스 데이터 통합 및 중복 제거
  - 실행: `python scripts/aggregator.py` 또는 `python -m scripts.aggregator`
  - 역할: 모든 키워드 아카이브 병합, 링크 기준 전역 중복 제거
  - 출력: 
    - `outputs/aggregated/canonical_archive.csv` (중복 제거된 최종본)
    

**핵심 함수 및 테스트 (공유 모듈)**
- `pipeline.py` - 뉴스 수집 파이프라인 (함수 단위)
  - 함수: `run_news_pipeline(keyword, total_count, is_keyword_required)`
  - 역할: 12단계 파이프라인 실행 (Fetch → Store → Filter → Scrape → Cluster → Final)
  - 호출처: `scripts/scheduler.py`, `main.py`

- `main.py` - 단일 키워드 테스트/디버깅
  - 실행: `python main.py`
  - 용도: 특정 키워드로 파이프라인 수동 실행 및 로컬 테스트

### 핵심 모듈
- api/naver_news_client.py
상태를 가지지 않는 API 클라이언트로, dict 리스트를 반환한다.

- api/news_repository.py
CSV 파일 관리와 링크 기반 증분 중복 제거를 담당한다.

- models/news_article_model.py
링크의 MD5 해시로 news_id를 자동 생성하는 데이터클래스이며, 타임존을 인식하는 타임스탬프를 포함한다.

- processors/news_cluster.py
제목과 본문 유사도를 이용한 그룹화와 대표 기사 선정을 수행한다.

- processors/article_similarity_grouper.py
TF-IDF와 코사인 유사도를 사용하며 임계값은 설정 가능하다.

- processors/news_filter.py
사전 및 사후 필터링 로직을 담당한다.

- processors/news_scraper.py
재시도 로직을 포함한 BeautifulSoup 기반 본문 추출 모듈이다.

### 설정 및 유틸리티
- config/init.py
네이버 ID/시크릿, Gemini API 키, 임계값, 키워드 등 모든 환경 변수를 로드한다.

- prompts/system_normal.txt
Gemini 2.5 Flash용 시스템 프롬프트이다.

- validators/
중복 여부, 필수 컬럼, 행 개수 등을 점검하는 데이터 품질 검증 모듈이다.

예컨대 scripts/aggregator.py는 drop_duplicates(subset=['link'])를 사용하여 URL이 같은 중복 기사를 하나만 남기고 모두 제거. 빠르고 명확하지만, 다른 언론사에서 발행한 내용은 거의 같지만 URL이 다른 '우라까이' 기사는 잡아내지 못한다.(실제로는 이 결과물이 임베딩에 사용됨) 다만, Keyword 별로 수집할 때는 제목유사도와 본문 유사도를 이미 체크했음.

validators/run_probe_global_similarity.py는 URL은 전혀 보지 않고, 오직 기사의 제목과 본문 내용을 벡터로 변환한 뒤 코사인 유사도를 계산. 이 유사도 점수가 config 파일에 설정된 PROBE_TITLE_THRESHOLD, PROBE_CONTENT_THRESHOLD 값보다 높으면 "사실상 같은 기사"일 가능성이 높다고 판단하고 그룹으로 묶는다. 결론적으로 aggregator.py는 1차적인 기계적 중복 제거를, run_probe_global_similarity.py는 내용 기반의 고차원적인 중복을 검증하는 역할을 수행한다. 다만 테스트용일 뿐 실전에서는 동작하지 않는다. 다른 기사를 다른 언론사에서 그대로 받아썼으면 중요도가 높다고 보기 때문.

---

## 프로젝트 전용 패턴과 규칙

### 1. 유사도 임계값의 중요성
TITLE_THRESHOLD = 0.20 # 제목 유사도 20% 초과 시 동일 기사 그룹으로 간주한다.
CONTENT_THRESHOLD = 0.15 # 본문 유사도 15% 초과 시 동일 기사 그룹으로 간주한다.

이 두 임계값은 OR 조건으로 결합된다. 즉, 제목 또는 본문 중 하나라도 임계값을 넘으면 동일 클러스터로 병합되며, 최초 기사 하나만 대표 기사로 남는다. 제목만 살짝 고친 우라까이 기사를 잡아내기 위해서다. 
만약, AND로 결합하면, 제목과 본문 모두가 임계값을 넘어야 동일 클러스터로 병합된다. 이러면 본문이 짧은 기사들은 유사도가 낮게 나오는 경향이 있다. 한국 언론의 기사 중에 제목이 비슷하지만 본문이 전혀 다른 기사는 극히 드물다. 그런 기사는 있어도 포기해도 된다. 

### 2. Group ID 의미
title_group_id(T-1, T-2 등)는 제목 유사도 기준의 임시 그룹이다.
cluster_id는 제목과 본문 기준을 결합한 최종 그룹이다.
content_group_id(C-1, C-2 등)는 본문을 공유하는 기사 기록용일 뿐, 의사결정 기준은 아니다.

### 3. 대표 기사 선정 로직
processors/canonical_news_policy.py를 참고한다.
클러스터 내부에서는 pubDate가 가장 최신인 기사 또는 URL 구조 우선순위로 선정한다.
전역 단계에서는 aggregator.py가 pubDate 기준 내림차순으로 최초 등장한 기사만 유지하고 이전 기사는 제거한다.

### 4. 키워드 설정
config/init.py에 다음과 같이 정의한다.

("이재명", False, 1)
("청와대", False, 1)

형식은 (키워드, is_keyword_required, fetch_hours)이다.
is_keyword_required=True이면 제목에 해당 키워드가 반드시 포함되어야 한다(엄격 모드).
fetch_hours는 스케줄 주기이며 1 이상이어야 한다.

### 5. 데이터 경로 규칙
- 키워드별 경로
outputs/<keyword>/raw_archive.csv
outputs/<keyword>/selected_archive.csv
outputs/<keyword>/filtered_logs/
outputs/<keyword>/similarity_logs/

- 전역 경로
outputs/aggregated/total_news_archive.csv
outputs/aggregated/total_news_archive_meta.csv

- 상태 관리
outputs/last_executed.json
execution_log.csv(누적 실행 통계)

### 6. 오류 처리 방식
- API 및 스크래핑 오류는 로그만 남기고 처리는 계속한다. 안정성을 우선한다.
- 데이터 검증은 validators/ 디렉터리의 독립 모듈에서 수행한다.
- 모든 이슈는 filtered_logs 또는 similarity_logs의 CSV로 저장해 감사 가능하게 한다.

---

## 디버깅 및 개발 워크플로

### 자주 사용하는 명령어

- 단일 키워드 테스트
python main.py (config에서 SEARCH_KEYWORDS를 하나만 남겨 실행)

- 전체 스케줄 실행
python scheduler.py

- 전역 병합
python aggregator.py

- 데이터 검증
python validators/run_all_validators.py

- RAG/LLM 테스트
python scripts/rag_test_e5.py

### 주요 디버깅 패턴
- 제목 유사도가 너무 높을 경우 .env에서 TITLE_THRESHOLD를 낮춘다.
- 기사가 누락된 경우 outputs/<keyword>/filtered_logs/step*.csv에서 제외 사유를 확인한다.
- 중복 매핑 문제가 의심되면 total_news_archive_meta.csv와 duplicate_removal_history.csv를 확인한다.
- 스크래핑 실패 시 NewsScraper의 재시도 로직과 타임아웃(기본 0.1초 지연)을 점검한다.

### 주로 확인하는 로그
- execution_log.csv: 키워드별 실행 통계(new_raw, final_added, timestamp)
- <keyword>/filtered_logs/stepN_*.csv: 필터 단계별 기사 제외 사유
- <keyword>/similarity_logs/YYYYMMDD_HHMM.csv: 상세 클러스터 할당 결과, 키워드별 클러스터링 로직은 이 파일을 보면 알 수 있다
- outputs/last_executed.json: 키워드별 마지막 실행 시각

## 연동 지점 및 외부 의존성

### API
- 네이버 뉴스 검색 API: .env에 NAVER_ID와 NAVER_SECRET이 필요하다.
- Google Gemini 2.5 Flash: llm/issue_labeler.py에서 프로파일링에 사용되며 GEMINI_API_KEY가 필요하다.

### 라이브러리
- 텍스트 유사도: scikit-learn(TF-IDF, 코사인 유사도)
- 스크래핑: BeautifulSoup4, requests, lxml
- 데이터 처리: pandas, numpy
- 로깅: CSV 기반 커스텀 로깅, 진행 표시용 tqdm

### 데이터 요구사항
- 필수 컬럼은 title, link(고유 ID), pubDate(대표 기사 선정 기준)이다.
- 선택적으로 content, originallink, description이 있으면 유용하다.

---

## 수정 가이드라인

### 새 필터 단계 추가

1. NewsFilter에 메서드를 추가한다. 예: apply_custom_filter()
2. main.py 파이프라인에서 5단계와 6단계 사이에 호출한다.
3. 제외된 기사는 outputs/<keyword>/filtered_logs/stepN_*.csv에 기록한다.
4. README.md의 테이블을 갱신한다.

### 새 클러스터링 방식 추가

1. ArticleSimilarityGrouper를 확장하거나 processors/에 새 클래스를 추가한다.
2. NewsCluster.process()에서 이를 사용하도록 수정한다.
3. similarity_logs/YYYYMMDD_HHMM.csv에 클러스터링 결과를 기록한다.
4. cluster_id, is_canon, replaced_by 컬럼은 반드시 유지한다(하위 단계 의존).

### 대표 기사 선정 로직 변경

1. CanonicalNewsPolicy의 메서드를 수정한다.
2. validators/check_global_canonical_consistency.py로 테스트한다.
3. is_canon 컬럼이 변경 사항을 정확히 반영하는지 확인한다.

---

## 테스트 및 검증

- 단위 테스트는 현재 없다. 통합 테스트와 검증 모듈에 의존한다.
- 통합 테스트는 소수 키워드로 전체 파이프라인을 실행하고 execution_log.csv 지표를 확인한다.
- 데이터 품질 검증은 전역 병합 후 validators/run_all_validators.py를 실행한다.
- 회귀 검증은 duplicate_removal_history.csv를 실행 간 비교해 예기치 않은 변화를 확인한다.

---

## Notes for AI Agents

**내가 작성한 코드의 의도와 변수명 유지**

- 스케줄러 로직을 수정하기 전에 항상 last_executed.json을 확인해 경합 상태를 방지한다.
- 대표 기사 선정은 결정적이다. 동일 입력이면 동일 출력이 나온다(pubDate가 동률일 경우 타이브레이커로 사용).
- 키워드 필터링은 선택 사항이다. is_keyword_required=False로 설정하면 관련 기사까지 포함하는 퍼지 매칭이 된다.
- CSV 인코딩은 UTF-8이다. 모든 파일 입출력에서 encoding="utf-8"을 명시해야 한다.
- 타임존은 일관성을 위해 pytz.timezone("Asia/Seoul")을 사용한다(KST는 UTC+9).

---

**사용자가 명시적으로 요청하지 않은 코드 수정이나 개선 제안을 먼저 하지 마시오.**
