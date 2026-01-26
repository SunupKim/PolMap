# scripts/scheduler.py
# 실제 메인 진입점 - 정기 수집 스케줄러
# 실행: 항상 프로젝트 루트에서만 실행하세요
#   python scripts/scheduler.py
#   또는
#   python -m scripts.scheduler
# 주의: 다른 디렉토리에서 실행하면 경로 오류 발생합니다

import json
import time
import os
import sys
import pandas as pd
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import run_news_pipeline
from config import SEARCH_KEYWORDS, TOTAL_FETCH_COUNT
from utils.logger import PipelineLogger, verify_file_before_write


# 통계 로그를 저장할 파일 경로
EXECUTION_LOG_PATH = "logs/execution_log.csv"
LAST_EXECUTED_PATH = "logs/last_executed.json"

def load_last_executed():
    if not os.path.exists(LAST_EXECUTED_PATH):
        return {}
    with open(LAST_EXECUTED_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_last_executed(data, backup_dir=None):
    # [백업] 기존 last_executed.json이 있다면, 이번 실행 로그 폴더로 이동(백업)
    if os.path.exists(LAST_EXECUTED_PATH) and backup_dir:
        backup_filename = f"last_executed.json.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        target_path = os.path.join(backup_dir, backup_filename)
        try:
            os.rename(LAST_EXECUTED_PATH, target_path)
        except Exception as e:
            print(f"[WARN] last_executed.json 백업 이동 실패: {e}")

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
            df_exist = pd.read_csv(EXECUTION_LOG_PATH)
            tot_new_raw = pd.to_numeric(df_exist["new_raw"], errors='coerce').sum()
            tot_final_added = pd.to_numeric(df_exist["final_added"], errors='coerce').sum()
        except Exception:
            pass
    
    # 데이터 정리
    log_data = []    
    for s in stats_list:
        new_raw = s["new_raw"]
        final_added = s["final_added"]
        
        tot_new_raw += new_raw
        tot_final_added += final_added
        
        ratio = round(final_added / new_raw, 3) if new_raw > 0 else 0
        
        log_data.append({
            "execute_at": now,
            "keyword": s["keyword"],
            "new_raw": new_raw,
            "final_added": final_added,
            "status": s["status"],
            "e_ratio": ratio,
            "tot_new_raw": int(tot_new_raw),
            "tot_final_added": int(tot_final_added)
        })
    
    df_log = pd.DataFrame(log_data)
    
    is_new = not os.path.exists(EXECUTION_LOG_PATH)
    os.makedirs(os.path.dirname(EXECUTION_LOG_PATH), exist_ok=True)
    
    with open(EXECUTION_LOG_PATH, 'a', encoding='utf-8-sig') as f:
        df_log.to_csv(f, index=False, header=is_new, lineterminator='\n')
        f.write(",,,,,,, \n")

def job():
    """전체 키워드 순회 및 수집 작업"""
    print(f"\n{'>'*10} 정기 수집 프로세스 시작: {datetime.now()} {'>'*10}")
    
    # 실행 시점별 로그 폴더 생성 (예: logs/20260126_0756)
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    current_log_dir = os.path.join("logs", run_timestamp)
    
    logger = PipelineLogger(log_dir=current_log_dir, module_name="scheduler")
    logger.start_step("스케줄 검사 및 파이프라인 실행", step_number=1, metadata={"log_dir": current_log_dir})
    now = datetime.now()

    last_executed = load_last_executed()
    updated_last_executed = dict(last_executed)

    all_stats = []
    executed_keywords = []

    for kw, is_required, fetch_per_hours in SEARCH_KEYWORDS:

        last_time_str = last_executed.get(kw)
        should_execute = False

        if not last_time_str:
            should_execute = True
            logger.add_metric(f"first_run_{kw}", True)
        else:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            elapsed_hours = (now - last_time).total_seconds() / 3600
            if elapsed_hours >= fetch_per_hours:
                should_execute = True
                logger.add_metric(f"interval_ok_{kw}", elapsed_hours)
            else:
                logger.add_metric(f"skipped_{kw}", f"elapsed {elapsed_hours:.1f}h < {fetch_per_hours}h")

        if not should_execute:
            continue

        try:
            stats = run_news_pipeline(kw, TOTAL_FETCH_COUNT, is_required, log_dir=current_log_dir)
            if stats:
                all_stats.append(stats)
                executed_keywords.append(kw)
                updated_last_executed[kw] = now.strftime("%Y-%m-%d %H:%M:%S")
                logger.add_metric(f"result_{kw}", stats["status"])

        except Exception as e:
            print(f"!!! [{kw}] 실행 중 오류: {e}")
            error_stat = {
                "keyword": kw, 
                "new_raw": 0,
                "final_added": 0,
                "status": f"error: {str(e)}"
            }
            all_stats.append(error_stat)
            logger.add_metric(f"error_{kw}", str(e))
    
    logger.add_metric("executed_keywords", executed_keywords)
    logger.end_step(result_count=len(executed_keywords))
    
    logger.start_step("실행 로그 저장", step_number=2)
    try:
        if all_stats:
            # 누적 로그이므로 백업(rename) 없이 append 모드로 바로 저장
            save_log_to_csv(all_stats)
            
            # last_executed는 덮어쓰기 전 현재 로그 폴더로 백업
            save_last_executed(updated_last_executed, backup_dir=current_log_dir)
            
            logger.end_step(result_count=len(all_stats))
        else:
            logger.end_step(result_count=0)
            print("[INFO] 실행 대상 키워드가 없습니다.")
            
    except PermissionError as e:
        logger.end_step(error=f"파일 저장 실패: {e}")
        print(f"!!! 로그 저장 중 오류: {e}")
        raise
    
    logger.save()
    print(f"{'<'*10} 정기 수집 완료 및 로그 기록 성공 {'<'*10}\n")

if __name__ == "__main__":
    INTERVAL_SECONDS = 60 * 60
    print(f"스케줄러 가동: 매시간 정기 수집 (INTERVAL: {INTERVAL_SECONDS}초)")
    
    while True:
        try:
            job()
        except KeyboardInterrupt:
            print("\n[INFO] 사용자 중단 신호 (Ctrl+C)")
            break
        except Exception as e:
            print(f"\n[CRITICAL] 스케줄러 오류: {e}")
            print(f"5초 후 재시작합니다...")
            time.sleep(5)
            continue
        
        time.sleep(INTERVAL_SECONDS)
