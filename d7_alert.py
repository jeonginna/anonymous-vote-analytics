"""
D+7 이탈 위험 스코어링 파이프라인 (운영 자동화 시뮬레이션)

흐름:
  1) mart_ml_d7_training 에서 '그날' 채점 대상 유저 + 피처 읽기
  2) GCS 에서 학습된 모델(final_rf_pipeline.joblib) 로드
  3) 예측 → risk_score → risk_segment 산출
  4) mart.ml_d7_prediction_result 에 멱등 적재 (그날 것 삭제 후 append)
  5) 적재 결과 검증 (행 수 / NULL / 확률 범위 / 위험구간 정렬)
  6) 알람: 태스크 실패 / 검증 경고(0건) / 적재 성공 시 Slack + 이메일 발송

과거 고정 데이터를 날짜별로 재생하는 시뮬레이션이라,
schedule 없이 수동 실행 + backfill 로 '매일 배치가 도는' 시나리오를 재현한다.

* 기존 테이블 스키마에 맞춤:
  scored_at (TIMESTAMP), first_hint_opened_in_24h (참고용 컬럼) 포함.
  모델 예측에는 first_hint_opened_in_24h 를 쓰지 않음(힌트 제외 모델).

* 알람 사전 설정 (Airflow UI):
  - Admin > Variables > slack_webhook_url   : Slack Incoming Webhook URL
  - Admin > Variables > alert_email_to      : 수신 이메일 (콤마로 복수 가능)
  - Admin > Connections > smtp_default      : SMTP 접속 정보 (host/port/login/password)
  둘 중 하나만 설정해도 그 채널로만 발송되고, 둘 다 없으면 알람은 로그 경고만 남기고 조용히 스킵된다.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText

import pendulum

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# 설정값
PROJECT_ID   = "your-gcp-project"
SOURCE_TABLE = f"{PROJECT_ID}.mart.mart_ml_d7_training"
TARGET_TABLE = f"{PROJECT_ID}.mart.ml_d7_prediction_result"
GCS_BUCKET   = "your-gcp-project-ml-artifacts"
MODEL_BLOB   = "d7_retention/current/final_rf_pipeline.joblib"
MODEL_VERSION = "rf_without_hint_v1"

FEATURE_COLUMNS = [
    "gender", "grade", "class_num", "school_type", "student_count",
    "ping_sent_count_24h", "ping_received_count_24h", "unique_sent_users_24h",
    "unique_received_users_24h", "active_hours_24h", "send_receive_ratio_24h",
    "category_count_24h", "dominant_category_24h", "max_category_share_24h",
    "category_diversity_24h", "category_0_ping_count_24h", "category_1_ping_count_24h",
    "category_2_ping_count_24h", "category_3_ping_count_24h", "category_4_ping_count_24h",
]
CATEGORICAL = ["gender", "grade", "class_num", "school_type", "dominant_category_24h"]
EXTRA_COLUMNS = ["first_hint_opened_in_24h"]  # 예측 미사용, 결과에 참고용으로 저장


# ---------------------------------------------------------------------------
# 알람 유틸
# ---------------------------------------------------------------------------

def _send_slack(message: str, color: str = "#439FE0") -> None:
    """Slack Incoming Webhook으로 알람 전송. 미설정 시 조용히 스킵."""
    import requests
    from airflow.models import Variable

    webhook_url = Variable.get("slack_webhook_url", default_var=None)
    if not webhook_url:
        log.info("slack_webhook_url 미설정 — Slack 알람 스킵")
        return
    payload = {"attachments": [{"color": color, "text": message}]}
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error("Slack 알람 전송 실패: %s", e)


def _send_email(subject: str, body: str) -> None:
    """smtp_default 커넥션으로 이메일 알람 전송. 미설정 시 조용히 스킵."""
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    to_addr_raw = Variable.get("alert_email_to", default_var=None)
    if not to_addr_raw:
        log.info("alert_email_to 미설정 — 이메일 알람 스킵")
        return
    try:
        conn = BaseHook.get_connection("smtp_default")
        to_addrs = [x.strip() for x in to_addr_raw.split(",") if x.strip()]

        msg = MIMEText(body, _charset="utf-8")
        msg["Subject"] = subject
        msg["From"] = conn.login
        msg["To"] = ", ".join(to_addrs)

        with smtplib.SMTP(conn.host, conn.port or 587, timeout=15) as server:
            server.starttls()
            server.login(conn.login, conn.password)
            server.sendmail(conn.login, to_addrs, msg.as_string())
    except Exception as e:
        log.error("이메일 알람 전송 실패: %s", e)


def _alert(message: str, subject: str, color: str = "#439FE0") -> None:
    """Slack + 이메일 동시 발송."""
    _send_slack(message, color=color)
    _send_email(subject, message)


def on_task_failure_alert(context) -> None:
    """DAG 전체 default_args에 걸어두는 실패 콜백. 어떤 태스크가 실패해도 호출된다."""
    ti = context["task_instance"]
    ds = context.get("ds")
    exception = context.get("exception")

    message = (
        f":red_circle: *[d7_retention_scoring] 태스크 실패*\n"
        f"- Task: `{ti.task_id}`\n"
        f"- Date: `{ds}`\n"
        f"- Try: {ti.try_number}\n"
        f"- Error: `{exception}`\n"
        f"- Log: {ti.log_url}"
    )
    _alert(
        message,
        subject=f"[실패] d7_retention_scoring / {ti.task_id} ({ds})",
        color="#FF0000",
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="d7_alert",
    start_date=pendulum.datetime(2023, 4, 28, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "d7", "retention"],
    default_args={
        # DAG 내 모든 태스크에 공통 적용되는 실패 콜백
        "on_failure_callback": on_task_failure_alert,
    },
)
def d7_retention_scoring():

    @task
    def load_targets(ds: str) -> str:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        select_cols = FEATURE_COLUMNS + EXTRA_COLUMNS
        query = f"""
            SELECT user_id, first_ping_at_kst, retained_d7, {", ".join(select_cols)}
            FROM `{SOURCE_TABLE}`
            WHERE DATE(first_ping_at_kst) = @ds
            ORDER BY first_ping_at_kst, user_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ds", "DATE", ds)]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        log.info("채점 대상 %s: %d명", ds, len(df))
        out_path = f"/tmp/targets_{ds}.parquet"
        df.to_parquet(out_path, index=False)
        return out_path

    @task
    def score(targets_path: str, ds: str) -> str:
        import numpy as np
        import pandas as pd
        import joblib
        from google.cloud import storage

        df = pd.read_parquet(targets_path)
        keep = [
            "user_id", "first_ping_at_kst", "actual_retained_d7",
            "retention_probability", "risk_score", "risk_segment",
            "first_hint_opened_in_24h", "model_version", "scored_at",
        ]
        if df.empty:
            log.warning("채점 대상 없음(%s) — 빈 결과", ds)
            out_path = f"/tmp/scored_{ds}.parquet"
            pd.DataFrame(columns=keep).to_parquet(out_path, index=False)
            return out_path

        for c in CATEGORICAL:
            df[c] = df[c].astype("string")

        local_model = f"/tmp/{MODEL_VERSION}.joblib"
        storage.Client(project=PROJECT_ID).bucket(GCS_BUCKET) \
            .blob(MODEL_BLOB).download_to_filename(local_model)
        model = joblib.load(local_model)

        proba = model.predict_proba(df[FEATURE_COLUMNS])[:, 1]
        df["retention_probability"] = proba
        df["risk_score"] = 1 - proba

        pct = pd.Series(df["risk_score"]).rank(pct=True, method="average")
        df["risk_segment"] = np.select(
            [pct >= 0.80, pct >= 0.50], ["HIGH", "MEDIUM"], default="LOW"
        )
        df["model_version"] = MODEL_VERSION
        df["scored_at"] = pd.Timestamp(ds, tz="Asia/Seoul")
        df = df.rename(columns={"retained_d7": "actual_retained_d7"})

        out_path = f"/tmp/scored_{ds}.parquet"
        df[keep].to_parquet(out_path, index=False)
        log.info("스코어링 완료 %s: %d행", ds, len(df))
        return out_path

    @task
    def load_to_bq(scored_path: str, ds: str) -> int:
        import pandas as pd
        from google.cloud import bigquery

        df = pd.read_parquet(scored_path)
        client = bigquery.Client(project=PROJECT_ID)

        client.query(
            f"DELETE FROM `{TARGET_TABLE}` WHERE DATE(scored_at, 'Asia/Seoul') = @ds",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ds", "DATE", ds)]
            ),
        ).result()

        if df.empty:
            log.warning("적재할 행 없음(%s)", ds)
            return 0

        client.load_table_from_dataframe(
            df, TARGET_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()
        log.info("적재 완료 %s: %d행 -> %s", ds, len(df), TARGET_TABLE)
        return len(df)

    @task
    def validate(row_count: int, ds: str) -> None:
        from google.cloud import bigquery

        if row_count == 0:
            log.warning("검증 스킵(%s): 적재 행 0", ds)
            _alert(
                f":warning: *[d7_retention_scoring] 검증 경고*\n"
                f"- Date: `{ds}`\n"
                f"- 적재된 행이 0건이라 검증을 스킵했습니다. 소스 데이터/조인 조건을 확인해주세요.",
                subject=f"[경고] d7_retention_scoring - 적재 0건 ({ds})",
                color="#FFCC00",
            )
            return

        client = bigquery.Client(project=PROJECT_ID)
        q = f"""
            SELECT
              COUNT(*) AS row_count,
              COUNT(*) AS total,
              COUNTIF(retention_probability IS NULL) AS prob_null,
              COUNTIF(retention_probability NOT BETWEEN 0 AND 1) AS prob_out_of_range,
              COUNTIF(risk_segment='HIGH')   AS high_cnt,
              COUNTIF(risk_segment='MEDIUM') AS mid_cnt,
              COUNTIF(risk_segment='LOW')    AS low_cnt,
              ROUND(AVG(IF(risk_segment='HIGH', actual_retained_d7, NULL)),3) AS high_ret,
              ROUND(AVG(IF(risk_segment='LOW',  actual_retained_d7, NULL)),3) AS low_ret
            FROM `{TARGET_TABLE}`
            WHERE DATE(scored_at, 'Asia/Seoul') = @ds
        """
        r = client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ds", "DATE", ds)]
            ),
        ).to_dataframe().iloc[0]

        assert r["prob_null"] == 0, f"확률 NULL 존재: {r['prob_null']}"
        assert r["prob_out_of_range"] == 0, f"확률 범위 오류: {r['prob_out_of_range']}"

        log.info(
            "검증 OK %s | 행:%d HIGH:%d MED:%d LOW:%d | 재발신율 HIGH:%s LOW:%s",
            ds, r["row_count"], r["high_cnt"], r["mid_cnt"], r["low_cnt"],
            r["high_ret"], r["low_ret"],
        )

        body = (
            f"D+7 이탈 위험 채점 리포트 - {ds}\n\n"
            f"전체 채점 인원: {r['total']}명\n"
            f"HIGH: {r['high_cnt']}명 (실제 재발신율 {r['high_ret']})\n"
            f"MEDIUM: {r['mid_cnt']}명\n"
            f"LOW: {r['low_cnt']}명 (실제 재발신율 {r['low_ret']})\n"
        )
        text = (
            f":white_check_mark: *[성공] d7_retention_scoring - 적재 완료 ({ds})*\n\n"
            f"📈 D+7 채점 리포트 - {ds}\n"
            f"• 전체: {r['total']}명\n"
            f"• HIGH: {r['high_cnt']}명 (실제 재발신율 {r['high_ret']})\n"
            f"• MEDIUM: {r['mid_cnt']}명\n"
            f"• LOW: {r['low_cnt']}명 (실제 재발신율 {r['low_ret']})"
        )

        _send_slack(text, color="#36A64F")
        _send_email(subject=f"[성공] d7_retention_scoring - 적재 완료 ({ds})", body=body)

    targets = load_targets()
    scored = score(targets)
    n = load_to_bq(scored)
    validate(n)


d7_retention_scoring()
