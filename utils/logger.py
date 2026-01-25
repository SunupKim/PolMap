import os
import json
import time
from datetime import datetime
from pathlib import Path

class PipelineLogger:
    """파이프라인 단계별 로깅 및 성능 측정 유틸"""
    
    def __init__(self, log_dir="outputs/Logs", module_name="default"):
        self.log_dir = log_dir
        self.module_name = module_name
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(log_dir, f"{module_name}_{self.timestamp}.json")
        self.steps = []
        self.current_step = None
        self.step_start_time = None
        
        os.makedirs(log_dir, exist_ok=True)
    
    def start_step(self, step_name, step_number=None, metadata=None):
        """단계 시작 기록"""
        self.current_step = {
            "step_number": step_number,
            "step_name": step_name,
            "start_time": datetime.now().isoformat(),
            "status": "running",
            "metadata": metadata or {},
            "metrics": {}
        }
        self.step_start_time = time.time()
        #print(f"[STEP {step_number}] {step_name} 시작...")
    
    def end_step(self, result_count=None, error=None):
        """단계 종료 기록"""
        if not self.current_step:
            return
        
        elapsed = time.time() - self.step_start_time
        self.current_step["end_time"] = datetime.now().isoformat()
        self.current_step["elapsed_seconds"] = round(elapsed, 2)
        self.current_step["status"] = "error" if error else "completed"
        
        if result_count is not None:
            self.current_step["metrics"]["result_count"] = result_count
        
        if error:
            self.current_step["error_message"] = str(error)
            print(f"[ERROR] {self.current_step['step_name']}: {error}")
        else:
            print(f"[STEP {self.current_step['step_number']}] {self.current_step['step_name']} 완료 ({elapsed:.2f}초, {result_count or 'N/A'}건)")
        
        self.steps.append(self.current_step)
        self.current_step = None
    
    def add_metric(self, key, value):
        """현재 단계에 메트릭 추가"""
        if self.current_step:
            self.current_step["metrics"][key] = value
    
    def save(self):
        """로그를 파일로 저장"""
        log_data = {
            "module": self.module_name,
            "timestamp": self.timestamp,
            "total_steps": len(self.steps),
            "steps": self.steps
        }
        
        with open(self.log_file, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n>>> 로그 저장: {self.log_file}")
        return self.log_file


def ensure_directory_writable(dirpath):
    """디렉토리가 쓰기 가능한지 확인"""
    try:
        os.makedirs(dirpath, exist_ok=True)
        # 쓰기 권한 테스트
        test_file = os.path.join(dirpath, ".write_test")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        return True, None
    except Exception as e:
        return False, str(e)


def verify_file_before_write(filepath):
    """파일 저장 전 경로 및 권한 검증"""
    dirpath = os.path.dirname(filepath)
    
    # 디렉토리 생성 및 권한 확인
    writable, error = ensure_directory_writable(dirpath)
    if not writable:
        raise PermissionError(f"디렉토리 쓰기 불가: {dirpath} - {error}")
    
    # 파일이 이미 존재하면 백업 생성
    if os.path.exists(filepath):
        backup_path = filepath + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            os.rename(filepath, backup_path)
            print(f"[INFO] 기존 파일 백업: {backup_path}")
        except Exception as e:
            raise PermissionError(f"기존 파일 백업 실패: {filepath} - {e}")
    
    return True


class ExecutionSummary:
    """전체 실행 결과 요약"""
    
    def __init__(self, summary_file="outputs/execution_summary.json"):
        self.summary_file = summary_file
        self.data = {}
    
    def record(self, keyword, stage, input_count, output_count, elapsed_seconds, status="success", error=None):
        """키워드별 단계 기록"""
        if keyword not in self.data:
            self.data[keyword] = []
        
        record = {
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "input_count": input_count,
            "output_count": output_count,
            "elapsed_seconds": elapsed_seconds,
            "status": status,
            "error_message": error
        }
        self.data[keyword].append(record)
    
    def save(self):
        """요약 저장"""
        os.makedirs(os.path.dirname(self.summary_file) or ".", exist_ok=True)
        with open(self.summary_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        print(f">>> 실행 요약 저장: {self.summary_file}")
