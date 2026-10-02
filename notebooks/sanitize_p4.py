#!/usr/bin/env python3
"""
P4 저장소 정리 스크립트
  1) 노트북 셀 출력 삭제 + 실행 카운트 초기화
  2) GCP 프로젝트 ID · 데이터셋 · 서비스 식별 테이블명 치환
  .py(Airflow DAG)는 출력이 없으므로 치환만 수행한다.

사용법:
  python sanitize_p4.py .            --dry-run     # 저장소 전체 미리보기
  python sanitize_p4.py notebooks/                 # 실제 적용
"""

import json
import re
import sys
import unicodedata
from pathlib import Path

# ── 치환 규칙: 긴 문자열부터 먼저 ──────────────────────────────
RENAME = {
    # GCP 프로젝트 · 버킷
    "project4-503701.votes":  "your-gcp-project.raw",   # 원천 데이터셋
    "project4-503701":        "your-gcp-project",

    # 서비스 식별 테이블 (A/B 솔루션명 제거)
    "stg_hackle_properties":  "stg_event_properties",
    "stg_hackle_events":      "stg_event_log",
    "hackle_properties":      "event_properties",
    "hackle_events":          "event_log",

    # 원천 앱 테이블
    "accounts_user":          "raw_user",
}

PATTERN = re.compile("|".join(re.escape(k) for k in sorted(RENAME, key=len, reverse=True)))


def sub(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    return PATTERN.sub(lambda m: RENAME[m.group(0)], text)


def process_ipynb(path: Path, dry_run: bool):
    nb = json.loads(path.read_text(encoding="utf-8"))
    n_out = n_sub = 0
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "code":
            n_out += len(cell.get("outputs", []))
            cell["outputs"] = []
            cell["execution_count"] = None
        src = cell.get("source", [])
        text = "".join(src) if isinstance(src, list) else src
        new = sub(text)
        if new != text:
            n_sub += 1
        cell["source"] = new.splitlines(keepends=True)
    if not dry_run:
        path.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return n_out, n_sub


def process_text(path: Path, dry_run: bool):
    text = path.read_text(encoding="utf-8")
    new = sub(text)
    n_sub = sum(1 for a, b in zip(text.splitlines(), new.splitlines()) if a != b)
    if new != text and not dry_run:
        path.write_text(new, encoding="utf-8")
    return 0, n_sub


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry_run = "--dry-run" in sys.argv
    if not args:
        print(__doc__)
        sys.exit(1)

    target = Path(args[0])
    files = []
    for ext in ("*.ipynb", "*.py", "*.md", "*.sql", "*.yml", "*.yaml"):
        files += sorted(target.rglob(ext)) if target.is_dir() else []
    if target.is_file():
        files = [target]
    files = [f for f in files
             if ".ipynb_checkpoints" not in str(f)
             and ".git/" not in str(f)
             and f.name != Path(__file__).name]

    if not files:
        print("대상 파일을 찾지 못했습니다.")
        sys.exit(1)

    tot_out = tot_sub = 0
    for f in files:
        n_out, n_sub = (process_ipynb if f.suffix == ".ipynb" else process_text)(f, dry_run)
        if n_out or n_sub:
            tag = "[미리보기] " if dry_run else ""
            print(f"{tag}{f.relative_to(target) if target.is_dir() else f.name}"
                  f"  출력 {n_out}개 · 치환 {n_sub}줄")
        tot_out += n_out
        tot_sub += n_sub

    print(f"\n총 {len(files)}개 파일 검사 · 출력 {tot_out}개 · 치환 {tot_sub}줄")
    if dry_run:
        print("--dry-run 이므로 파일은 저장되지 않았습니다.")


if __name__ == "__main__":
    main()
