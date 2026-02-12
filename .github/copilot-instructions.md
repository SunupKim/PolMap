**사용자가 명시적으로 요청하지 않은 코드 수정이나 개선 제안을 먼저 하지 마시오.**

# PolMap2 AI Guidelines

**CRITICAL: 사용자가 명시적으로 요청하지 않은 코드 수정이나 개선 제안을 먼저 하지 마시오.**

## 1. System Context
PolMap2는 데이터 품질(중복 제거)을 최우선으로 하는 한국 정치 뉴스 수집 파이프라인이다.

- **Fetch**: `NaverNewsClient` (API 호출)
- **Store**: `NewsRepository` (증분 저장, Link 기반 1차 중복 방지)
- **Pre-Filter**: `NewsFilter` (제목 패턴, 스니펫 길이, 네이버 뉴스 여부 필터링)
- **Scrape**: `NewsScraper` (BS4 본문 추출)
- **Post-Filter**: `NewsFilter` (본문 길이, 품질 필터링)
- **Cluster**: `NewsCluster` (유사도 기반 그룹화 및 대표 기사 선정)
- **Global Dedupe**: `aggregator.py` (전역 통합 및 Link 기반 최종 중복 제거)

---

## 2. Core Logic & Rules

### 유사도 및 클러스터링 (OR 조건)
- **기준**: `TITLE_THRESHOLD`(0.20) **OR** `CONTENT_THRESHOLD`(0.15) 중 하나라도 만족하면 동일 그룹.
- **목적**: 제목만 살짝 바꾼 '우라까이' 기사를 잡아내기 위함.
- **결과**: `cluster_id`로 묶이며, `is_canon=True`인 기사 하나만 생존. 나머지는 `replaced_by`로 매핑.

### 중복 제거 및 검증 정책
1. **Aggregator (`scripts/aggregator.py`)**:
   - **실전용**. `link`(URL)가 100% 일치하는 기사만 기계적으로 제거.
   - 다른 언론사의 '받아쓰기' 기사는 URL이 다르므로 **제거하지 않고 남겨둠** (중요도 반영).
2. **Similarity Probe (`validators/run_global_similarity_probe.py`)**:
   - **검증용**. URL이 달라도 내용(제목/본문)이 유사한 기사를 찾아냄.
   - 실제 삭제는 하지 않고, "사실상 같은 기사"가 얼마나 남았는지 모니터링하는 용도.

---

## 3. File Structure & Paths

- **Entry Points**:
  - `scripts/scheduler.py`: 정기 수집 루프 (Logs: `logs/YYYYMMDD_HHMM/`)
  - `scripts/aggregator.py`: 전역 병합 (Logs: `logs/aggregator_YYYYMMDD_HHMM/`)
  - `main.py`: 단일 키워드 테스트
- **Logic**: `pipeline.py` (오케스트레이션), `processors/` (핵심 로직)
- **Data Paths**:
  - 개별 키워드: `archive/<kw>/selected_archive.csv`
  - 최종 결과: `archive/aggregated/canonical_archive.csv`
  - 로그: `logs/execution_log.csv`, `logs/aggregation_stats.csv`

---

## 4. Coding Standards

1. **Encoding**: 모든 파일 입출력은 `utf-8` (CSV는 `utf-8-sig`) 사용.
2. **Timezone**: `pytz.timezone("Asia/Seoul")` (KST) 통일.
3. **State Safety**: `scheduler.py` 수정 시 `last_executed.json` 로직 보존 필수.
4. **Error Handling**: API/Scraping 오류 발생 시 로그만 남기고 파이프라인은 계속 진행(Fail-safe).
5. **No Unsolicited Changes**: 사용자가 요청한 부분 외의 코드는 건드리지 않는다.


20260209

[완료]
canonical_archive.csv
  → 기사 임베딩
  → 시간창 필터
  → KMeans
  → issue_meta.json / issue_centers.json

[지금 하는 일]
issue_meta.json
  → 사람이 보는 형태로 노출

[아직 안 하는 것]
- 이슈 간 거리 시각화
- PolMap 좌표와 결합
- 이슈 타임라인
