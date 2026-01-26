# 실행법 python validators/run_all_validators.py

import subprocess
import sys

VALIDATORS = [
    "validators/check_no_duplicate_links.py",
    "validators/check_required_columns.py",
    "validators/check_global_canonical_consistency.py",
    "validators/check_row_count_sanity.py",
    "validators/check_canonical_policy_execution.py",
]

def run_validator(path: str) -> int:
    print(f"\n--- 실행: {path} ---")
    result = subprocess.run(
        [sys.executable, path],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    return result.returncode

def main():
    for validator in VALIDATORS:
        code = run_validator(validator)

        if code == 0:
            print(f"[OK] {validator} 통과")
            continue

        if code == 2:
            print(f"[WARN] {validator} 경고 상태")
            sys.exit(2)

        print(f"[FAIL] {validator} 실패, 이후 검증 중단")
        sys.exit(1)

    print("\n모든 데이터 무결성 검증 통과")
    sys.exit(0)

if __name__ == "__main__":
    main()
