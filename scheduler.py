# scheduler.py

import json
import time
import os
import pandas as pd
from datetime import datetime
from main import run_news_pipeline
from config import SEARCH_KEYWORDS, TOTAL_FETCH_COUNT


# 통계 로그를 저장할 파일 경로
EXECUTION_LOG_PATH = "outputs/execution_log.csv"
LAST_EXECUTED_PATH = "outputs/last_executed.json"

def load_last_executed():
    if not os.path.exists(LAST_EXECUTED_PATH):
        return {}
    with open(LAST_EXECUTED_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_last_executed(data):
    os.makedirs(os.path.dirname(LAST_EXECUTED_PATH), exist_ok=True)
    with open(LAST_EXECUTED_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_log_to_csv(stats_list):
    """실행 결과를 누적 로그 파일에 저장"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 기존 파일이 있으면 읽어와서 누적값 계산 준비
    tot_new_raw = 0
    tot_final_added = 0

    if os.path.exists(EXECUTION_LOG_PATH):
        try:
            # 빈 줄(구분줄)이 섞여 있을 수 있으므로 skip_blank_lines 옵션 활용
            df_exist = pd.read_csv(EXECUTION_LOG_PATH)
            # 숫자가 아닌 행(구분줄 등)을 제외하고 합계 계산
            tot_new_raw = pd.to_numeric(df_exist["new_raw"], errors='coerce').sum()
            tot_final_added = pd.to_numeric(df_exist["final_added"], errors='coerce').sum()
        except Exception:
            pass
    
    # 데이터 정리
    log_data = []    
    for s in stats_list:
        # main.py에서 반환하는 키값(new_raw_count, final_added_count)과 일치시킴
        new_raw = s["new_raw"]
        final_added = s["final_added"]
        
        # 누적값 업데이트
        tot_new_raw += new_raw
        tot_final_added += final_added
        
        # 수집 효율(Ratio) 계산 (0으로 나누기 방지)
        ratio = round(final_added / new_raw, 3) if new_raw > 0 else 0
        
        log_data.append({
            "execute_at": now,
            "keyword": s["keyword"],
            "new_raw": new_raw,
            "final_added": final_added,
            "status": s["status"],
            "e_ratio": ratio,      # 추가: final_added_count / new_raw_count
            "tot_new_raw": int(tot_new_raw), # 추가: raw 누적값
            "tot_final_added": int(tot_final_added) # 추가: final 누적값
        })
    
    df_log = pd.DataFrame(log_data)
    
    # 파일이 없으면 새로 만들고, 있으면 이어서 저장(append)
    # 파일 저장 및 구분줄 추가
    is_new = not os.path.exists(EXECUTION_LOG_PATH)
    os.makedirs(os.path.dirname(EXECUTION_LOG_PATH), exist_ok=True)
    
    with open(EXECUTION_LOG_PATH, 'a', encoding='utf-8-sig') as f:
        # 1. 데이터 기록 (첫 생성일 때만 헤더 포함)
        df_log.to_csv(f, index=False, header=is_new, lineterminator='\n')
        # 2. 싸이클 종료 후 구분줄 추가 (요청사항 1번)
        f.write(",,,,,,, \n") # 컬럼 개수만큼 콤마를 넣어 CSV 구조 유지

def job():
    """전체 키워드 순회 및 수집 작업"""
    print(f"\n{'>'*10} 정기 수집 프로세스 시작: {datetime.now()} {'>'*10}")
    now = datetime.now()

    last_executed = load_last_executed()
    updated_last_executed = dict(last_executed)

    all_stats = []

    for kw, is_required, fetch_per_hours in SEARCH_KEYWORDS:

        # 키워드별 실행 주기 체크
        last_time_str = last_executed.get(kw)

        if last_time_str:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            elapsed_hours = (now - last_time).total_seconds() / 3600
            if elapsed_hours < fetch_per_hours:
                continue

        try:
            stats = run_news_pipeline(kw, TOTAL_FETCH_COUNT, is_required)
            if stats:
                all_stats.append(stats)
                updated_last_executed[kw] = now.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"!!! [{kw}] 실행 중 오류: {e}")
            all_stats.append({
                "keyword": kw, 
                "new_raw": 0,         # new_raw_count -> new_raw로 변경
                "final_added": 0,     # final_added_count -> final_added로 변경
                "status": f"error: {str(e)}"
            })           
    
    if all_stats:
        save_log_to_csv(all_stats)
        save_last_executed(updated_last_executed)

    print(f"{'<'*10} 정기 수집 완료 및 로그 기록 성공 {'<'*10}\n")

if __name__ == "__main__":
        
    INTERVAL_SECONDS = 60 * 60
    while True:
        job()
        time.sleep(INTERVAL_SECONDS)