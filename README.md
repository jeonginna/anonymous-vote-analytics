# 청소년 익명 투표 앱 리텐션 분석

> 익명 투표형 SNS의 신규 유저 행동을 분석해  
> **가입 초기 어떤 경험이 이후 재발신과 연결되는지** 확인하고,  
> `Staging → EDA → Mart → 통계 검정 → ML → Airflow` 흐름으로 분석을 확장한 프로젝트이다.

---

## 목차

- [한 줄 결론](#한-줄-결론)
- [1. Project Overview](#1-project-overview)
- [2. Analysis Flow](#2-analysis-flow)
- [Dashboards](#dashboards)
- [3. Data Preparation](#3-data-preparation)
- [4. EDA Summary](#4-eda-summary)
- [5. Hypothesis](#5-hypothesis)
- [6. Retention & Statistical Validation](#6-retention--statistical-validation)
- [7. Machine Learning](#7-machine-learning)
- [8. Key Findings](#8-key-findings)
- [9. Business Implication](#9-business-implication)
- [10. Implementation Status](#10-implementation-status)
- [11. Additional Analysis](#11-additional-analysis)
- [12. Limitations](#12-limitations)
- [13. Repository Structure](#13-repository-structure)
- [14. Reproducibility](#14-reproducibility)
- [15. Next Step](#15-next-step)

## 한 줄 결론

초기 분석에서는 **PING 수신 후 힌트까지 열람한 사용자**가 힌트를 열지 않은 사용자보다 이후 재발신율이 높았다.  
다만 머신러닝 단계에서 초기 활동량과 사용자 특성을 함께 고려했을 때 힌트 피처의 독립적인 기여는 크지 않았다.

따라서 특정 기능 하나보다 **가입 직후 실제 행동을 시작하고, 초기 활동을 충분히 만드는 것**이 이후 유지와 더 밀접한 신호라고 해석했다.

---

## 1. Project Overview

### 분석 질문

> 가입 초기의 어떤 행동과 경험이 이후 재발신과 연결되는가?  
> 특히 PING 수신 이후의 **힌트 열람 경험**은 이후 행동과 어떤 관계가 있는가?

### 서비스 핵심 행동

- **PING**: 특정 질문에 친구를 선택해 보내는 익명 투표
- **받은 PING**: 다른 사용자가 나를 선택해 보낸 투표
- **힌트**: 하트를 사용해 PING 발신자의 이름 일부를 확인하는 기능
- **재발신**: 가입 당일 이후 다시 PING을 보내는 행동

### 기술 스택

- **Data Warehouse**: BigQuery
- **Data Pipeline**: Local MySQL → GCS → BigQuery
- **Analysis**: BigQuery SQL, Python, Pandas
- **Modeling**: scikit-learn, LightGBM
- **Collaboration**: GitHub
- **Orchestration / Extension**: Airflow

---

## 2. Analysis Flow

```text
서비스·데이터 구조 이해
        ↓
Staging 테이블 구축
        ↓
데이터 품질 점검 / EDA
        ↓
분석 목적별 Mart 구축
        ↓
활성화·재발신 관계 분석
        ↓
가설 검정
        ↓
ML용 Base / Feature / Training Mart 구축
        ↓
D+7 유지 예측 모델링
        ↓
사용자별 유지확률·위험점수 산출
```

BigQuery 내부에서는 다음과 같이 레이어를 분리했다.

```text
Raw
 ↓
Stage
 ↓
Mart
 ↓
Feature / Training Mart
 ↓
Prediction
```

- **Raw**: 원본 데이터 보존
- **Stage**: 타입, 시간대, 코드값, 중복 등을 정리해 분석 가능한 형태로 표준화
- **Mart**: 분석 질문에 맞춘 사용자·행동 단위 테이블
- **Feature / Training Mart**: ML 학습 시점에 맞춘 사용자 단위 피처 테이블
- **Prediction**: 유지확률, 이탈 위험점수 등 모델 결과 저장

---

## Dashboards

분석 결과와 운영 지표는 Looker Studio 대시보드로 시각화했다. 두 대시보드 모두 BigQuery Mart를 데이터 소스로 직접 연결했다.

| 대시보드 | 내용 | 연결 Mart | 링크 |
| --- | --- | --- | --- |
| **Activation & Retention Dashboard** | 활성화·누적 재발신율·코호트 리텐션 등 분석 결과 | `mart_retention_cohort` 등 | [바로가기]([https://datastudio.google.com/s/hget61nyr7Q](https://datastudio.google.com/reporting/cac796de-949f-4e07-8cd7-4f230b46ab41)) |
| **Daily Operations Dashboard** | DAU 및 신규 가입 추이, PING 발신·힌트 열람·결제·탈퇴 추이 | `mart_ops_daily` 등 | [바로가기]([https://datastudio.google.com/s/jRoJ9CN0B1c](https://datastudio.google.com/reporting/b90b082d-85db-4940-b3d9-5ecc818ddc4f)) |

---

## 3. Data Preparation

### Staging

원천 데이터를 바로 분석하지 않고, 테이블별 Staging 노트북을 만들어 정제·표준화했다.

주요 Staging 테이블은 다음과 같다.

```text
stg_user
stg_vote
stg_question
stg_group
stg_school
stg_payment
stg_pointhistory
stg_withdraw
stg_event_log
stg_event_properties
stg_user_properties
```

### 전처리 원칙

- 원천 레코드를 임의로 삭제하지 않고 최대한 보존
- 중복·결측·이상치는 발생 원인과 분석 영향을 확인한 뒤 처리
- 분석 모집단 필터링은 Stage가 아니라 분석 목적에 맞는 Mart 단계에서 적용
- 사용자 ID, 이벤트 시간, 조인 키의 타입과 정합성을 우선 검증
- 데이터 수집 공백은 활동이 없었던 기간이 아니라 **관찰 불가 구간**으로 구분

---

## 4. EDA Summary

EDA에서는 사용자 규모와 구성, 친구 관계, Activation Funnel, 초기 활동량, 리텐션 등을 폭넓게 확인했다.

모든 EDA를 최종 분석에 직접 사용한 것은 아니며, 탐색 결과를 바탕으로 분석 범위를 **가입 초기 경험과 이후 재발신 관계**로 좁혔다.

### 주요 발견

- 전체 유효 분석 사용자 중 최종 Activation 정의를 만족한 사용자는 **5,059명**
- 학교 가입자 규모가 커질수록 평균 친구 수가 증가하다 일정 규모 이후 완만해지는 패턴이 나타남
- 친구 수가 많은 사용자는 활동 총량은 높았지만, 받은 PING 대비 개별 반응률은 반드시 높지 않았음
- 가입 초기 행동량에 따라 이후 D+7 유지 지표에 큰 차이가 나타남

### 가입 초기 활동과 D+7

| 가입 당일 행동 | D+7 리텐션 |
| --- | ---: |
| 행동 없음 | **11.45%** |
| 1회 | **54.22%** |
| 2~3회 | **59.02%** |

EDA 단계에서 특정 기능 하나보다 **초기 행동 자체의 강도**가 중요한 설명 신호일 가능성을 확인했다.

---

## 5. Hypothesis

### 가설

> **D0에 PING을 수신하고 힌트를 열람한 유저는,  
> PING을 수신했지만 힌트를 열람하지 않은 유저보다 이후 후속 PING 발신율이 높을 것이다.**

### 그룹 정의

세 그룹 모두 가입 당일(D0)에 PING을 발신한 사용자로 한정했다.

| Group | 정의 | 인원 |
| --- | --- | ---: |
| G0 | D0 발신 O / 수신 X | 284 |
| G1 | D0 발신 O / 수신 O / 힌트 X | 1,002 |
| G2 | D0 발신 O / 수신 O / 힌트 O | 2,483 |

- **대표 비교: G2 vs G1**
  - 두 그룹 모두 PING을 수신했기 때문에 힌트 열람 여부에 초점을 맞출 수 있음
- **보조 비교: G1 vs G0**
  - PING 수신 경험 자체와 이후 행동의 관계를 확인

### 리텐션 정의

이 프로젝트에서는 앱 실행 여부가 아니라 **후속 PING 발신 여부**를 리텐션 지표로 사용했다.

- D+0 행동은 후속 재발신 계산에서 제외
- D+1 / D+3 / D+7 / D+10 누적 재발신 여부 관찰
- 각 시점까지 관찰 가능한 사용자만 분석

---

## 6. Retention & Statistical Validation

활성화 사용자 기준 누적 재발신율은 다음과 같았다.

| 관찰 시점 | 누적 재발신 사용자 | 누적 재발신율 |
| --- | ---: | ---: |
| D+1 | 3,137 | 62.0% |
| D+3 | 3,339 | 66.0% |
| D+7 | 3,393 | 67.1% |
| D+10 | 3,412 | 67.4% |

D+10까지 재발신한 사용자 중 대부분이 이미 D+1 안에 행동해, **가입 직후가 반복 행동 형성에 중요한 구간**임을 확인했다.

> 📊 활성화·코호트 리텐션 지표는 [Activation & Retention Dashboard]([https://datastudio.google.com/s/j9Fg-Icypcc](https://datastudio.google.com/reporting/cac796de-949f-4e07-8cd7-4f230b46ab41))에서 확인할 수 있다.

### G2 vs G1

- D+1 / D+3 / D+7 / D+10에서 G2의 재발신율이 G1보다 일관되게 높게 나타남
- 여러 시점을 동시에 비교하는 문제를 고려해 다중 비교 보정을 적용
- 이변량 분석 수준에서는 힌트 열람과 이후 재발신 사이의 유의한 연관을 확인

### 해석

이 결과만으로 **힌트 열람이 재발신을 직접 유발했다**고 볼 수는 없다.

힌트를 열람하는 사용자가 원래 더 적극적으로 서비스를 이용하는 사용자일 수 있기 때문이다.  
따라서 다음 단계에서는 초기 활동량과 사용자 특성을 함께 사용하는 머신러닝 분석으로 확장했다.

---

## 7. Machine Learning

### 목적

첫 PING 발신 이후 초기 행동을 바탕으로 **D+7 유지 여부를 예측**하고,

1. 초기 활동 중 어떤 정보가 예측에 기여하는지 확인
2. 유지 가능성이 낮은 사용자를 상대적으로 구분

하는 것을 목표로 했다.

### ML 데이터 설계

- **분석 단위**: 사용자 1명 = 1행
- **최종 Base / Feature Mart 사용자**: **4,844명**
- **기준 시점(T0)**: `first_ping_at`
- **피처 관찰 구간**: T0 이후 첫 24시간
- **타깃**: `retained_d7`
- **분할 방식**: `first_ping_at` 기준 시간순 Train / Validation / Test
- **전처리**: Train 데이터에만 fit
- **Test 데이터**: 모델 선택 완료 후 마지막 평가에만 사용

피처 테이블은 최종적으로 `row_count = user_count = 4,844`, `duplicate_count = 0`을 만족하도록 QA했다.

### 주요 피처 그룹

- 사용자 기본 정보
- 첫 24시간 PING 행동
- 첫 수신 및 최초 힌트 열람 시점
- 질문 카테고리 행동
- 포인트 관련 행동

힌트 데이터의 경우 전체 힌트 열람 이벤트를 직접 관찰할 수 없었기 때문에,  
`first_hint_opened_at`을 활용해 **서비스 최초 힌트 열람이 T0 이후 24시간 안에 발생했는지**를 피처로 정의했다.

### 모델링 흐름

```text
Dummy
  ↓
Logistic Regression
  ↓
Random Forest
  ↓
LightGBM
  ↓
Ablation Test
  ↓
시간순 교차검증 기반 튜닝
  ↓
Final Test
```

Validation에서는 Random Forest가 후보 모델 중 가장 높은 ROC-AUC를 보였고, LightGBM은 Train 대비 Validation 성능 격차가 커 과적합 신호가 확인됐다.

### Ablation Test

피처 그룹을 하나씩 제거해 특정 정보가 실제 일반화 성능에 기여하는지 확인했다.

- 사용자 기본 정보 제거 시 성능 하락이 가장 크게 나타남
- 힌트 관련 피처 제거 시 성능이 오히려 소폭 개선
- PING·질문 카테고리 정보의 단독 기여는 제한적

> 이변량 분석에서 강하게 보였던 **힌트 열람 ↔ 재발신 관계**는  
> 다른 초기 활동 정보를 함께 고려했을 때 독립적인 예측 신호로 강하게 남지 않았다.

### 최종 산출물

모델 결과는 단순 0/1 예측뿐 아니라 다음 형태로 만들었다.

- `retention_probability`: D+7 유지 확률
- `risk_score`: 상대적 이탈 위험점수
- `risk_segment`: 상대적 위험구간

이 점수는 인과 효과나 절대적인 미래 확률이라기보다 **사용자 간 관리 우선순위를 구분하는 상대적 지표**로 해석했다.

---

## 8. Key Findings

### 1) 재발신은 가입 초기에 집중됐다

D+10 누적 재발신율만 보면 67.4%로 높아 보이지만, 재발신 사용자의 행동은 가입 직후에 크게 집중됐다.

따라서 누적 지표만 보기보다 **언제 첫 반복 행동이 발생했는지**를 함께 보는 것이 중요했다.

### 2) 초기 활동 강도가 이후 유지와 밀접하게 연결됐다

가입 당일 행동이 없던 사용자와 실제 행동을 시작한 사용자 사이에서 D+7 지표에 큰 차이가 나타났다.

### 3) 힌트 열람은 재발신과 연관됐지만 독립적인 효과로 단정할 수 없었다

G2는 G1보다 이후 재발신율이 높았지만, 머신러닝의 Ablation 결과에서는 힌트 정보의 독립적인 예측 기여가 크지 않았다.

### 4) 하나의 기능보다 사용자 초기 행동 전체를 함께 보는 것이 중요했다

최종적으로 특정 기능 하나를 핵심 원인으로 단정하기보다,  
**가입 초기에 사용자가 실제 행동을 시작했는지와 얼마나 적극적으로 활동했는지**를 함께 보는 방향이 더 적절하다고 판단했다.

---

## 9. Business Implication

분석 결과를 바탕으로 다음 방향을 제안했다.

- 가입 직후 첫 PING 발신까지의 마찰을 줄이는 온보딩
- 초기 행동이 부족한 사용자 조기 식별
- 수신·힌트 열람 같은 단일 기능 KPI뿐 아니라 초기 행동량을 함께 모니터링
- 사용자 위험점수를 절대 예측값이 아닌 운영 우선순위 기준으로 활용
- 힌트 열람 유도의 실제 효과는 A/B Test로 별도 검증

> 본 프로젝트는 관찰 데이터를 기반으로 하므로 특정 행동의 **인과 효과를 증명한 분석이 아니다.**

---

## 10. Implementation Status

분석에서 그친 것이 아니라, 예측 결과를 실제 운영에 활용할 수 있는 형태까지 구축했다.

- **예측 결과 저장 구조 구축**: 사용자별 유지확률·위험점수·위험구간을 `ml_d7_prediction_result`에 적재. 같은 날짜를 다시 실행해도 중복되지 않도록 멱등(그날 데이터 삭제 후 재적재) 처리
- **위험구간 운영 기준 정의**: 위험점수 기준 상위 20%를 HIGH, 이후 구간을 MEDIUM / LOW로 구분. HIGH 구간의 실제 D+7 재발신율이 LOW 구간보다 뚜렷하게 낮아, 상대적 관리 우선순위 지표로 활용 가능함을 확인
- **운영 자동화 파이프라인 구현**: Airflow DAG(`d7_alert.py`)로 대상 조회 → 스코어링 → 적재 → 검증 → 알람(Slack·이메일)까지 하나의 흐름으로 구성. 과거 고정 데이터를 날짜별로 재생하는 배치 시뮬레이션 형태로 구현

> 현재 데이터는 정적 덤프이므로 실시간 운영이 아니라, 실운영 배치를 모의하는 시뮬레이션으로 구현했다.

---

## 11. Additional Analysis

핵심 리텐션 분석 외에도 서비스와 데이터를 이해하기 위해 다음 주제를 탐색했다.

- 학교 규모와 친구 관계
- 친구 수에 따른 행동 차이
- 첫 하트 구매 시점과 반복 구매 패턴
- 신고·차단·숨김 행동이 높은 사용자 집단
- 운영 KPI 및 일일 지표

이 분석들은 서비스 전반을 이해하는 EDA로 활용했고,  
최종 리텐션 분석의 핵심 결론과는 구분해 정리했다.

---

## 12. Limitations

- **관찰 데이터**: 무작위 배정이 없어 인과 추론에 한계가 있음
- **교란 가능성**: 활동량, 학교 네트워크, 학년, 학교 유형 등의 영향이 남을 수 있음
- **데이터 수집 공백**: 일부 기간은 실제 활동 여부를 완전히 관찰할 수 없음
- **소스 간 매칭 한계**: 데이터 소스에 따라 개인 단위 매칭 신뢰도가 낮은 구간이 있음
- **리텐션 정의**: 앱 재방문이 아니라 후속 PING 발신을 핵심 행동으로 정의
- **힌트 피처 한계**: 반복 힌트 열람 전체가 아니라 `first_hint_opened_at` 기반 최초 경험을 사용
- **ML 모델 한계**: 원인 설명 모델이 아니라 D+7 유지 여부를 구분하기 위한 예측 모델

---

## 13. Repository Structure

```text
project/
├── .gitignore
├── README.md
├── requirements.txt
│
├── airflow/
│   ├── dags
│       ├── d7_alert.py
│   ├── docker-compose.yml
│   ├── requirements.txt
│
├── docs/
│   ├── diagrams
│   ├── guides
│   ├── reports
│
└── notebooks/
    ├── 01_Stage/
    │   ├── stg_user.ipynb
    │   ├── stg_vote.ipynb
    │   ├── stg_question.ipynb
    │   ├── ...
    │   └── stg_withdraw.ipynb
    │
    ├── 02_EDA/
    │   ├── high_report_block_hide_users.ipynb
    │   ├── signup_friend_count_behavior.ipynb
    │   ├── user_base_overview.ipynb
    │   ├── zero_friend_users.ipynb
    │
    ├── 03_Marts/
    │   ├── mart_acquisition.ipynb
    │   ├── mart_activation.ipynb
    │   ├── mart_retention.ipynb
    │   └── for_ops_dashboard.ipynb
    │
    └── 04_ML/
        ├── churn_prediction_base_mart.ipynb
        ├── fct_ml_ping_24h.ipynb
        ├── fct_ml_hint_24h.ipynb
        ├── fct_ml_category_24h.ipynb
        ├── mart_ml_d7_training.ipynb
        └── ml_d7_evaluation_and_simulation.ipynb
```

---

## 14. Reproducibility

이 저장소는 **다른 로컬 환경에서도 분석 흐름을 이어갈 수 있는 수준**으로 정리하는 것을 목표로 한다.

### 실행 환경

- Python
- Jupyter Notebook 또는 VS Code
- Google Cloud / BigQuery 접근 권한

### 권장 실행 순서

```text
1. BigQuery 연결 및 권한 확인
        ↓
2. 01_stage 노트북 실행
        ↓
3. 분석에 필요한 Mart 생성
        ↓
4. EDA / 리텐션 분석 및 통계 검정
        ↓
5. ML Base Mart 생성
        ↓
6. 24시간 행동 Feature 생성
        ↓
7. Training Mart 생성
        ↓
8. 모델링 및 예측 결과 생성
```

### Python 환경

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

macOS / Linux:

```bash
source .venv/bin/activate
```

필요 패키지는 `requirements.txt`로 설치한다.

```bash
pip install -r requirements.txt
```

### BigQuery 인증

로컬 환경에서는 Google Cloud Application Default Credentials를 사용할 수 있다.

```bash
gcloud auth application-default login
```

> 실제 데이터에는 사용자·서비스 관련 정보가 포함되어 있어 원천 데이터는 GitHub에 공개하지 않는다.  
> 따라서 전체 분석 재실행에는 해당 BigQuery 프로젝트의 접근 권한이 필요하다.

---

## 15. Next Step

이 프로젝트는 관찰 데이터 기반 분석과 운영 자동화 시뮬레이션까지 완료했다. 실무로 확장한다면 위 한계를 넘어서는 다음 단계로 이어질 수 있다.

- **인과 검증 (↔ 관찰 데이터 한계)**: 초기 활동·힌트 열람의 실제 효과는 무작위 배정 기반 온보딩 A/B Test로 검증
- **연속 수집 환경 재측정 (↔ 데이터 수집 공백 한계)**: 관찰 불가 구간이 없는 연속 데이터에서 리텐션 지표를 다시 측정
- **실운영 전환 (↔ 정적 덤프 기반 시뮬레이션)**: 현재 배치 시뮬레이션을 실데이터 연속 적재 기반 스케줄 파이프라인으로 전환하고, 재학습 트리거 기준(성능 저하·데이터 드리프트 등)을 정의
- **모델 고도화**: 최종 피처 세트를 주기적으로 재검증하고, 위험구간 기준을 운영 결과에 따라 보정

---

## Data Notice

본 저장소에는 개인정보 또는 식별 가능한 원천 사용자 데이터를 포함하지 않는다.  
포트폴리오에서는 분석 코드, 데이터 구조 설명, 집계 결과만 공개한다.
