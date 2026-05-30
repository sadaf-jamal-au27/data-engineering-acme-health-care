# Enterprise Healthcare Insurance Data Engineering Platform on Google Cloud Platform

**Production Architecture, Implementation & Operations Handbook**

Version 1.0 · Document classification: Internal / Reference · Audience: beginner → advanced data engineers, architects, and platform operators

---

## How to read this document

This handbook is written so a beginner can follow it top to bottom and an experienced engineer can jump to any section. Three conventions run throughout:

- **WHY / WHAT / HOW boxes.** For every meaningful step you'll see *why* we do it, *what problem it solves*, *what the service does internally*, and *how real enterprises use it*.
- **`[SCREENSHOT]` placeholders.** Because this is a text handbook, every place a real screenshot belongs is marked `[SCREENSHOT: …]` with a precise description of what should appear on screen, which buttons to click, the expected output, and the common error state. Drop your captured image directly beneath the placeholder.
- **Copy-ready artifacts.** gcloud commands, SQL, Python, and DAGs are real and runnable (adjust project IDs / regions). Code blocks are the deliverable, not illustrations.

> **A note on cost figures.** Cloud pricing changes constantly. This document gives *relative* cost guidance and the levers that move your bill, not dated dollar amounts. Always confirm live prices in the GCP Pricing Calculator before committing budget.

---

## Table of contents

1. Executive Summary
2. Business & Domain Primer (Healthcare Insurance)
3. Reference Architecture (all 10 platforms)
4. Technology Stack & Rationale
5. GCP Foundation: Projects, Org Layout, IAM Bootstrap
6. Cloud Storage: The Landing Zone
7. Cloud Composer Deep Dive (and what it builds internally on GKE)
8. Apache Airflow Internals
9. BigQuery Medallion Architecture (Bronze / Silver / Gold)
10. Pub/Sub & the Streaming / Real-Time Path
11. Pipeline Catalog — the 20 DAGs
12. Fully Implemented DAGs (5) + the Reusable Pattern
13. Data Quality, PII Masking, Audit & Compliance
14. Looker Semantic & Dashboard Layer
15. Security & HIPAA-style Controls
16. Monitoring, Logging, Alerting & Incident Response
17. CI/CD Architecture
18. Cost Optimization, Budget Alerts & Cleanup
19. Troubleshooting Guide
20. Scaling Strategies
21. Future Enhancements
22. Resume / Project Description
23. Interview Questions & Answers
24. Real Enterprise Use Cases
25. Appendix: Folder Structures & Glossary

---

# 1. Executive Summary

A health insurer ("payer") runs on data it does not generate cleanly. Claims arrive from thousands of hospitals, clinics, and pharmacies in inconsistent formats. Premium payments flow in daily. Members enroll, switch plans, and churn. Providers submit, resubmit, and occasionally defraud. Regulators demand auditable lineage and strict handling of Protected Health Information (PHI). Executives want a single, trustworthy number for "loss ratio" by 9 a.m.

This platform turns that chaos into governed, query-ready, real-time analytics on Google Cloud Platform. It is organized around **ten capability platforms**:

1. **Batch ETL Platform** — scheduled ingestion and transformation of claims, payments, enrollment, and provider data.
2. **Streaming ETL Platform** — sub-minute ingestion of claim events via Pub/Sub.
3. **Fraud Detection Platform** — batch scoring plus a real-time alert path.
4. **Payments Analytics Platform** — premium collection, reconciliation, and revenue KPIs.
5. **Claims Analytics Platform** — claim adjudication outcomes, denial rates, cost drivers.
6. **Executive KPI Platform** — medical loss ratio (MLR), membership, revenue, fraud exposure.
7. **Member Enrollment Analytics** — acquisition, churn, plan mix.
8. **Provider Analytics** — provider cost, quality, and outlier behavior.
9. **Real-Time Fraud Alerts** — streaming detection that pages an investigator within seconds.
10. **Monitoring & Alerting Platform** — the platform that watches all the other platforms.

The engine is **Cloud Composer** (managed Apache Airflow) orchestrating **20+ DAGs**, landing raw data in **Cloud Storage**, loading and transforming it through a **BigQuery medallion architecture** (Bronze → Silver → Gold), streaming events through **Pub/Sub**, and exposing curated marts through **Looker**. **IAM**, **Cloud Monitoring**, and **Cloud Logging** provide governance and observability; **Cloud SQL** backs operational/reference data and Composer's metadata; **GKE** runs under Composer (and can host custom services).

**Business outcomes the platform is built to deliver:**

- Cut the time-to-insight for claims and loss-ratio reporting from days to hours.
- Detect likely-fraudulent claims in batch nightly *and* flag high-risk claim events in seconds.
- Provide a single governed source of truth ("Gold") that every dashboard reads from.
- Make every transformation auditable and every PHI field controlled, to support HIPAA-style compliance.
- Keep cloud spend predictable through partitioning, clustering, autoscaling, and disciplined teardown.

---

# 2. Business & Domain Primer (Healthcare Insurance)

If you have never worked with payer data, read this section first. The pipelines downstream only make sense once the vocabulary does.

**The core entities**

- **Member (Subscriber / Beneficiary):** the insured person. Has a `member_id`, demographics, and a plan.
- **Plan / Policy:** the insurance product the member bought (e.g., HMO, PPO). Has premiums, deductibles, and coverage rules.
- **Premium:** the recurring amount the member (or employer) pays to keep coverage active.
- **Provider:** a doctor, hospital, clinic, or pharmacy that delivers care. Identified by an **NPI** (National Provider Identifier).
- **Claim:** a request for payment submitted by a provider after care is delivered. Contains diagnosis codes (**ICD-10**), procedure codes (**CPT/HCPCS**), amounts billed, and the service date.
- **Adjudication:** the insurer's decision on a claim — paid, partially paid, denied, or pended — and the reason codes attached.
- **Denial / Rejection:** a claim the insurer will not pay, with a reason (e.g., not covered, duplicate, missing prior authorization).
- **PHI / PII:** Protected Health Information and Personally Identifiable Information — names, SSNs, member IDs tied to diagnoses. Strictly regulated.

**The KPIs executives actually ask for**

- **Medical Loss Ratio (MLR):** claims paid ÷ premiums earned. The single most important payer metric. Regulators require minimums (a payer that spends too little on care must rebate members).
- **Denial Rate:** denied claims ÷ total claims. High denial rates create regulatory and member-satisfaction risk.
- **Churn Rate:** members who lapse ÷ total members.
- **Fraud Exposure:** estimated dollars at risk from suspicious claims.
- **Days to Adjudicate:** operational efficiency of claim processing.

**Why fraud detection matters here.** Healthcare fraud — phantom billing, upcoding (billing a more expensive procedure than performed), unbundling, and duplicate claims — costs the industry tens of billions annually. Patterns worth flagging: a provider whose average claim is a statistical outlier, the same procedure billed many times for one member in a short window, services billed on dates a member could not have received them, and providers whose claim volume spikes abnormally.

Keep these definitions handy; the Silver and Gold SQL later encodes them directly.

---

# 3. Reference Architecture (all 10 platforms)

## 3.1 The 30,000-foot view

```
                          ┌─────────────────────────────────────────────────────────┐
                          │                  SOURCE SYSTEMS                           │
                          │  Hospitals · Clinics · Pharmacies · Billing · CRM · Bank  │
                          └───────────────┬───────────────────────┬─────────────────-┘
                                          │ (files, SFTP, API)     │ (events)
                                          ▼                        ▼
                       ┌──────────────────────────┐     ┌────────────────────────┐
                       │   CLOUD STORAGE (GCS)     │     │     PUB/SUB TOPICS      │
                       │  Landing / Raw buckets    │     │  claim-events, payments│
                       └────────────┬─────────────┘      └───────────┬───────────-┘
                                    │                                 │
              ┌─────────────────────▼─────────────────────┐          │ (BQ subscription /
              │            CLOUD COMPOSER (Airflow)        │          │  Dataflow)
              │   20+ DAGs orchestrate batch + sensors     │          │
              │   (runs on a managed GKE cluster)          │          │
              └─────────────────────┬─────────────────────┘          │
                                    │ load + transform                │
                                    ▼                                 ▼
        ┌───────────────────────────────────────────────────────────────────────────┐
        │                              BIGQUERY                                       │
        │   BRONZE (raw, append) → SILVER (clean, conformed) → GOLD (marts, KPIs)     │
        └───────────────────────────────┬─────────────────────────────────-----------┘
                                         │
                ┌────────────────────────┼─────────────────────────┐
                ▼                        ▼                          ▼
        ┌──────────────┐      ┌────────────────────┐      ┌───────────────────┐
        │    LOOKER     │      │  REAL-TIME ALERTS  │      │  CLOUD MONITORING  │
        │  Dashboards   │      │  (Pub/Sub → page)  │      │  + LOGGING + SLAs  │
        └──────────────┘      └────────────────────┘      └───────────────────┘

   Cross-cutting:  IAM · Service Accounts · VPC/Private IP · Secret Manager · DLP · CMEK
```

## 3.2 How the ten platforms map onto this picture

| Platform | Primary GCP services | Layer it lives in |
|---|---|---|
| 1. Batch ETL | Composer, GCS, BigQuery | Ingestion + Bronze/Silver |
| 2. Streaming ETL | Pub/Sub, BigQuery (BQ subscription) / Dataflow | Ingestion + Bronze |
| 3. Fraud Detection (batch) | Composer, BigQuery, (optional) Vertex AI | Silver/Gold scoring |
| 4. Payments Analytics | Composer, BigQuery, Cloud SQL | Silver/Gold |
| 5. Claims Analytics | Composer, BigQuery | Silver/Gold |
| 6. Executive KPI | BigQuery (Gold), Looker | Gold + Semantic |
| 7. Member Enrollment Analytics | Composer, BigQuery | Silver/Gold |
| 8. Provider Analytics | Composer, BigQuery | Silver/Gold |
| 9. Real-Time Fraud Alerts | Pub/Sub, Cloud Functions/Run, Monitoring | Streaming + Alerting |
| 10. Monitoring & Alerting | Cloud Monitoring, Logging, Composer | Cross-cutting |

## 3.3 Data flow narrative (the "happy path")

1. A hospital drops a daily claims file into an SFTP endpoint; an integration job copies it to a **GCS landing bucket** under a dated prefix.
2. A **Composer DAG** (`claims_pipeline`) detects the file with a GCS sensor, validates it, and loads it *as-is* into a **Bronze** BigQuery table (full fidelity, append-only, with ingestion metadata).
3. A Silver task **cleans and conforms** the data: standardizes codes, deduplicates, casts types, masks PHI where required, and writes to a **Silver** table partitioned by service date.
4. A Gold task **aggregates** Silver into business marts — denial rates, MLR inputs, provider cost summaries — that **Looker** reads.
5. In parallel, claim *events* publish to a **Pub/Sub topic**; a **BigQuery subscription** streams them into a Bronze streaming table, and a lightweight scorer flags high-risk events to a **fraud-alerts topic**, which pages an investigator.
6. **Cloud Monitoring** watches DAG success, freshness, and cost; **Cloud Logging** retains the audit trail.

The rest of this handbook builds each box, top to bottom.

---

# 4. Technology Stack & Rationale

| Component | Role on this platform | Why it (and not an alternative) |
|---|---|---|
| **GCP** | Cloud foundation | Native, managed services for every layer; strong data-warehouse story (BigQuery). |
| **Cloud Composer** | Orchestration (managed Airflow) | Schedules, retries, and sequences 20+ pipelines; managed so you don't run Airflow yourself. |
| **Apache Airflow** | Workflow engine inside Composer | Industry-standard DAG model; rich operator ecosystem for GCP. |
| **Cloud Storage (GCS)** | Landing zone / data lake | Cheap, durable, decoupled ingestion buffer; the boundary between "outside" and "inside." |
| **BigQuery** | Serverless data warehouse | Separates storage from compute, scales to petabytes, SQL-native medallion layers. |
| **Pub/Sub** | Streaming message bus | Decouples event producers from consumers; at-least-once delivery; native BQ subscriptions. |
| **Looker** | Semantic + BI layer | Governed metrics in LookML; one definition of "MLR" for the whole company. |
| **Python / Pandas / PyArrow** | Transformation & validation glue | Pandas for in-DAG validation; PyArrow for efficient Parquet I/O. |
| **SQL** | The heart of Silver/Gold transforms | Pushes compute to BigQuery; declarative, auditable, fast. |
| **GKE** | Runs under Composer; optional custom services | Composer provisions a GKE cluster for the Airflow components. |
| **Cloud SQL** | Reference/operational data; (Composer metadata is managed) | Relational store for slowly changing dimensions and app state. |
| **IAM** | Access control | Least-privilege, per-service-account permissions. |
| **Cloud Monitoring / Logging** | Observability | Metrics, dashboards, alerts, audit logs. |

**Beginner clarification — "isn't BigQuery just a database?"** Not quite. A traditional database couples storage and compute on the same machine, so it gets slower and pricier as data grows. BigQuery stores data in a columnar format on Google's distributed file system and spins up *thousands* of workers on demand for a single query, then releases them. You pay for storage (cheap) and the bytes scanned per query (the lever you optimize with partitioning and clustering, covered in §9).

---

# 5. GCP Foundation: Projects, Org Layout, IAM Bootstrap

## 5.1 WHY this comes first

**WHY:** Everything you build inherits the identity, permissions, and billing boundary of the project it lives in. Get this wrong and you'll either over-permission (a security finding) or under-permission (pipelines that fail at 2 a.m.).
**WHAT problem it solves:** clean separation of environments (dev/test/prod), predictable billing, and least-privilege access.
**HOW enterprises do it:** separate GCP *projects* per environment under a shared *folder* in the org, with a dedicated billing account and centrally managed service accounts.

## 5.2 Recommended project layout

```
Organization: acme-health.com
└── Folder: data-platform
    ├── Project: acme-health-data-dev
    ├── Project: acme-health-data-test
    └── Project: acme-health-data-prod
```

## 5.3 Console navigation

1. Go to **console.cloud.google.com**.
2. Top bar → **project selector** → **New Project**.
3. Name it `acme-health-data-dev`, attach the billing account, place it under the `data-platform` folder.

`[SCREENSHOT: GCP "New Project" dialog. You should see fields for Project name, auto-generated Project ID, Billing account dropdown, and Location (folder). Click CREATE. Expected output: a notification "Creating project…" then the new project appears in the selector. Error case: "You do not have permission to create projects" → you lack resourcemanager.projects.create at the folder/org level; ask an org admin.]`

## 5.4 Enable the APIs (gcloud)

```bash
# Set your working project
gcloud config set project acme-health-data-dev

# Enable everything the platform needs
gcloud services enable \
  composer.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  pubsub.googleapis.com \
  container.googleapis.com \
  sqladmin.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  dlp.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com
```

**WHAT this does internally:** "Enabling an API" provisions the service's control-plane endpoints for your project and unlocks the relevant quotas and IAM roles. Until enabled, calls return `SERVICE_DISABLED`.

## 5.5 Service accounts (the platform's machine identities)

**WHY:** Pipelines should never run as a human. Each workload gets a dedicated **service account (SA)** with only the permissions it needs.

```bash
# A dedicated SA for Composer / Airflow workloads
gcloud iam service-accounts create composer-runner \
  --display-name="Composer Airflow runner"

# Grant least-privilege roles (scope tighter in prod via custom roles)
PROJECT=acme-health-data-dev
SA="composer-runner@${PROJECT}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/pubsub.subscriber"
```

**IAM roles explained (the ones you'll actually use):**

| Role | Grants | Why the platform needs it |
|---|---|---|
| `roles/composer.worker` | Run Composer environment workloads | Attached to the Composer SA. |
| `roles/bigquery.dataEditor` | Read/write table data (not delete datasets) | DAGs load and transform tables. |
| `roles/bigquery.jobUser` | Run query/load jobs | Required to execute any SQL. |
| `roles/storage.objectAdmin` | Read/write/delete objects in buckets | Land files, archive, clean up. |
| `roles/pubsub.subscriber` / `publisher` | Consume / emit messages | Streaming ingestion and alerts. |
| `roles/monitoring.metricWriter` | Emit custom metrics | DAGs publish freshness/quality metrics. |
| `roles/secretmanager.secretAccessor` | Read secrets | DB passwords, API keys. |

**Principle of least privilege in practice:** start from the table above, then in prod replace broad predefined roles with **custom roles** that contain only the specific permissions your tasks call. Never grant `roles/owner` or `roles/editor` to a service account.

## 5.6 Repository / folder structure for the codebase

```
acme-health-data-platform/
├── dags/
│   ├── batch/
│   │   ├── claims_pipeline.py
│   │   ├── payments_pipeline.py
│   │   ├── member_enrollment_pipeline.py
│   │   ├── provider_analytics_pipeline.py
│   │   ├── hospital_claims_pipeline.py
│   │   ├── pharmacy_claims_pipeline.py
│   │   ├── premium_analytics_pipeline.py
│   │   ├── policy_renewal_pipeline.py
│   │   ├── rejected_claims_pipeline.py
│   │   ├── customer_churn_pipeline.py
│   │   └── executive_kpi_pipeline.py
│   ├── fraud/
│   │   ├── fraud_detection_pipeline.py
│   │   └── real_time_fraud_alert_pipeline.py
│   ├── streaming/
│   │   └── streaming_claims_pipeline.py
│   ├── governance/
│   │   ├── data_quality_pipeline.py
│   │   ├── pii_masking_pipeline.py
│   │   ├── audit_pipeline.py
│   │   ├── compliance_pipeline.py
│   │   └── monitoring_pipeline.py
│   └── orchestration_master_pipeline.py
├── sql/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── plugins/
│   └── common/                # shared operators, callbacks, validation helpers
├── include/
│   └── config/                # per-env YAML (datasets, buckets, thresholds)
├── lookml/                    # Looker project (views, explores, models)
├── tests/                     # pytest for DAG import + unit tests on helpers
├── ci/
│   └── cloudbuild.yaml
└── README.md
```

This is the standard Composer layout: the `dags/` tree is what gets synced into the environment's DAGs bucket (see §7.6).

---

# 6. Cloud Storage: The Landing Zone

## 6.1 WHY / WHAT / HOW

**WHY:** Source systems are messy and bursty. You need a cheap, durable buffer between "the outside world" and your warehouse so ingestion failures never corrupt analytics data.
**WHAT problem it solves:** decoupling. Files land first, get validated, then load. If a load fails, the raw file is still there to retry — you never lose source data.
**WHAT GCS does internally:** stores objects redundantly across a region (or multi-region), with strong consistency, versioning, lifecycle rules, and per-object IAM/encryption.
**HOW enterprises use it:** a tiered bucket strategy — a *landing* bucket for raw drops, a *raw/archive* bucket for retained originals, and lifecycle rules that move old data to cheaper storage classes and eventually delete it.

## 6.2 Bucket design

| Bucket | Purpose | Storage class | Lifecycle |
|---|---|---|---|
| `acme-health-landing-dev` | New files arrive here | Standard | Delete after 7 days (already loaded) |
| `acme-health-raw-dev` | Immutable archive of originals | Standard → Nearline → Coldline | Nearline @ 30d, Coldline @ 90d, delete @ 7y (retention policy) |
| `acme-health-tmp-dev` | Scratch / staging for transforms | Standard | Delete after 2 days |

> Healthcare retention is often **7 years**; encode that as a bucket **retention policy** so objects cannot be deleted early even by an admin.

## 6.3 Create buckets and lifecycle (gcloud)

```bash
REGION=us-central1
gcloud storage buckets create gs://acme-health-landing-dev   --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create gs://acme-health-raw-dev       --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create gs://acme-health-tmp-dev       --location=$REGION --uniform-bucket-level-access

# Lifecycle rule for the raw archive
cat > lifecycle.json << 'JSON'
{
  "rule": [
    {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},  "condition": {"age": 30}},
    {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},  "condition": {"age": 90}}
  ]
}
JSON
gcloud storage buckets update gs://acme-health-raw-dev --lifecycle-file=lifecycle.json
```

`[SCREENSHOT: Cloud Storage → Buckets list. You should see the three buckets with Location "us-central1", Storage class "Standard", and "Public access: Not public". Click a bucket → Lifecycle tab to see the rules. Error case: "AccessDeniedException: 403" → the SA/user lacks roles/storage.admin to create buckets.]`

## 6.4 Folder (prefix) convention inside landing

```
gs://acme-health-landing-dev/
└── claims/
    └── ingest_date=2025-05-31/
        └── hospital_claims_20250531.csv
└── payments/
    └── ingest_date=2025-05-31/
└── enrollment/
```

Date-partitioned prefixes make idempotent re-runs trivial: a DAG run for `2025-05-31` only ever touches that prefix.

---

# 7. Cloud Composer Deep Dive (and what it builds internally on GKE)

This is the section most people skip and later regret. Understanding what Composer *actually provisions* explains your bill, your failure modes, and your scaling knobs.

## 7.1 What Cloud Composer is

**WHY:** You need something to run pipelines on a schedule, retry failures, enforce ordering, and give you a UI and logs — without operating Airflow servers yourself.
**WHAT it is:** Cloud Composer is **managed Apache Airflow**. Google runs the Airflow components for you on a managed GKE cluster, handles upgrades and the metadata database, and exposes the Airflow web UI.
**WHAT problem it solves:** running production Airflow (scheduler HA, worker autoscaling, a hardened metadata DB, secure web access) is genuinely hard. Composer makes it a configuration exercise.
**HOW enterprises use it:** one Composer environment per environment tier (dev/test/prod), with DAGs deployed via CI/CD, private networking, and Airflow connections/variables managed centrally.

## 7.2 What Composer creates internally (the part nobody tells beginners)

When you create a Composer 2 environment, Google provisions, on your behalf:

```
Cloud Composer Environment (logical)
├── GKE cluster (Autopilot or standard)            ← compute that runs Airflow
│   ├── Airflow Scheduler   (decides what runs when)
│   ├── Airflow Workers     (execute the tasks; autoscale)
│   ├── Airflow Triggerer   (handles "deferrable" / async tasks)
│   ├── Airflow Web Server   (the UI you log into)
│   └── DAG Processor        (parses your .py files into DAG objects)
├── Cloud SQL instance (managed, hidden)           ← the Airflow METADATA DB
├── A Cloud Storage bucket                          ← DAGs, plugins, logs, data
│   ├── /dags        (your DAG files sync here)
│   ├── /plugins
│   ├── /data
│   └── /logs
├── A Pub/Sub-style internal control channel
└── Service account + IAM bindings
```

**GKE internals — why it matters.** The Airflow components are Kubernetes pods. Workers autoscale based on queued tasks. If you "over-parallelize" a DAG, you spawn more worker pods and your bill rises. If a worker pod is OOM-killed, its task is marked failed and retried. Knowing this turns mysterious failures into understood ones.

**Component responsibilities:**

| Component | Job | Failure symptom |
|---|---|---|
| **Scheduler** | Reads the metadata DB, decides which task instances are ready, queues them | Tasks stuck in "scheduled"/"queued" forever |
| **DAG Processor** | Parses `.py` files into DAGs every few seconds | New/edited DAGs don't appear in UI; import errors banner |
| **Workers** | Pull queued tasks and execute the operator code | Tasks fail with worker logs; autoscaling lag |
| **Triggerer** | Runs `async`/deferrable operators efficiently (e.g., long sensors) | Deferrable tasks never resume |
| **Web Server** | Serves the Airflow UI | Can't reach UI; 502s |
| **Metadata DB (Cloud SQL)** | Stores DAG/task state, XComs, connections, variables | Everything stalls; this is the brain |

## 7.3 Create a Composer environment (console)

1. **Console → Composer → Create environment → Composer 2**.
2. Set name `acme-composer-dev`, location `us-central1`.
3. Choose the `composer-runner` service account.
4. Set environment size **Small** for dev; configure scheduler/worker counts.
5. (Prod) enable **Private IP** and **Private environment**.

`[SCREENSHOT: Composer "Create environment" form. You should see Name, Location, Image version (composer-2.x.x-airflow-2.x.x), Service account, Environment resources (Small/Medium/Large), and a Networking section with "Private IP" toggle. Click CREATE. Expected: the environment shows a spinner and takes ~20–25 minutes to become green/Healthy. Error case: "Composer API has not been used in project…" → enable composer.googleapis.com first; "Service account does not have composer.worker" → add the role.]`

`[SCREENSHOT: Composer environments list once ready. You should see acme-composer-dev with a green check, columns for Location, Image version, and links "Airflow UI" and "DAGs folder" (the GCS bucket). Clicking "Airflow UI" opens the Airflow web interface.]`

## 7.4 Create via gcloud (reproducible)

```bash
gcloud composer environments create acme-composer-dev \
  --location=us-central1 \
  --image-version=composer-2.9.0-airflow-2.9.1 \
  --service-account=composer-runner@acme-health-data-dev.iam.gserviceaccount.com \
  --environment-size=small
```

## 7.5 Networking & private Composer (prod)

**WHY:** PHI must not traverse the public internet, and the Airflow UI should not be world-reachable.
**HOW:** a **private IP** Composer environment places the GKE nodes and the metadata DB on a VPC with no public IPs. Egress to Google APIs goes through **Private Google Access**; admin access to the UI is gated behind **Identity-Aware Proxy (IAP)** or a VPN/bastion. Configure a dedicated VPC + subnet, set secondary ranges for pods/services, and enable Private Google Access on the subnet.

## 7.6 The DAGs bucket & synchronization

Composer maps your DAGs to a GCS path. Deploying a DAG is literally copying a file:

```bash
# Find the DAGs bucket
gcloud composer environments describe acme-composer-dev \
  --location us-central1 --format="value(config.dagGcsPrefix)"

# Deploy a DAG (CI/CD does this for you)
gcloud composer environments storage dags import \
  --environment acme-composer-dev --location us-central1 \
  --source dags/batch/claims_pipeline.py
```

The DAG Processor notices the new file within seconds and the DAG appears in the UI.

## 7.7 Scaling strategies (real-world)

- **Worker autoscaling:** set min/max workers; Composer 2 scales worker pods with queue depth.
- **Parallelism knobs:** Airflow config `parallelism`, `max_active_tasks_per_dag`, `max_active_runs_per_dag` cap concurrency so a backfill can't starve the cluster.
- **Right-size the environment:** start Small in dev, Medium/Large in prod; scale the scheduler count if DAG parsing/scheduling lags.
- **Offload heavy compute:** never crunch large data inside a worker pod with Pandas. Push it to BigQuery (a SQL operator) so the worker only orchestrates. This is the single biggest scaling lesson.
- **Deferrable operators:** use them for long waits (sensors) so you don't pin a worker slot for hours.

---

# 8. Apache Airflow Internals

## 8.1 The mental model

A **DAG** (Directed Acyclic Graph) is a Python file that defines **tasks** and the **dependencies** between them. Airflow turns it into scheduled **DAG runs**, each containing **task instances** with their own state machine (`scheduled → queued → running → success/failed/up_for_retry`).

```
DAG: claims_pipeline (schedule: daily @ 02:00)
   wait_for_file >> validate >> load_bronze >> build_silver >> build_gold >> quality_check >> notify
```

## 8.2 Key concepts you must know

- **Operator:** a unit of work (`BigQueryInsertJobOperator`, `GCSToBigQueryOperator`, `PythonOperator`). One operator → one task.
- **Sensor:** an operator that waits for a condition (e.g., `GCSObjectExistenceSensor` waits for a file). Prefer `mode="reschedule"` or deferrable sensors to free worker slots.
- **XCom:** "cross-communication" — a small key/value passed between tasks via the metadata DB. Use for tiny values (a row count, a file name), never for data payloads.
- **Connections & Variables:** stored in the metadata DB; reference credentials/config without hardcoding. Back sensitive ones with **Secret Manager**.
- **Hooks:** the client libraries operators use to talk to services (`BigQueryHook`).
- **`catchup`:** if `True`, Airflow backfills every missed schedule between `start_date` and now. Set `catchup=False` unless you intend to backfill.
- **Idempotency:** a task should produce the same result if re-run. Use `WRITE_TRUNCATE` on partitions or `MERGE` so retries don't double-load.
- **Retries & SLAs:** `retries`, `retry_delay`, and `sla` per task; `on_failure_callback` to alert.

## 8.3 TaskFlow vs classic operators

Modern Airflow supports the **TaskFlow API** (`@task` decorators) for Python logic and classic operators for service calls. The DAGs in §12 use classic operators for clarity and because most heavy lifting is SQL pushed to BigQuery.


---

# 9. BigQuery Medallion Architecture (Bronze / Silver / Gold)

## 9.1 What the medallion architecture is and why payers love it

**WHY:** Raw source data is untrustworthy and changes shape. Analysts need clean, stable, fast tables. Auditors need to prove what the data looked like on arrival. You cannot satisfy all three with one table.
**WHAT problem it solves:** it splits responsibilities into three layers so each has one job:

- **Bronze (Raw):** the data exactly as it arrived, append-only, with ingestion metadata. *Truth of record.* Never edited. Lets you reprocess and audit.
- **Silver (Clean / Conformed):** typed, deduplicated, standardized, PHI-masked-where-required, joined to reference data. *The trusted working layer.*
- **Gold (Marts / KPIs):** business-level aggregates and dimensional marts that Looker reads. *The presentation layer.*

**HOW enterprises use it:** every dashboard reads Gold; every Gold table is reproducible from Silver; every Silver row traces to Bronze. If a KPI looks wrong, you debug downward through the layers.

## 9.2 Dataset layout

```
acme-health-data-dev (project)
├── bronze_claims, bronze_payments, bronze_enrollment, bronze_providers
├── silver  (conformed tables: claims, payments, members, providers)
├── gold    (marts: kpi_executive, fraud_scores, denial_summary, provider_cost, churn)
└── ref     (reference/dimension data: plans, icd10, cpt, provider_master)
```

```bash
for ds in bronze_claims bronze_payments bronze_enrollment bronze_providers silver gold ref; do
  bq --location=US mk --dataset --description "Medallion: $ds" acme-health-data-dev:$ds
done
```

`[SCREENSHOT: BigQuery Studio → Explorer pane. You should see the project expanded with datasets bronze_claims, silver, gold, ref. Clicking a dataset shows its tables; clicking a table shows the Schema, Details (partitioning/clustering), and Preview tabs. Error case: "Not found: Dataset" → wrong region; datasets are region-scoped, ensure queries run in the same location.]`

## 9.3 Partitioning & clustering (the cost & speed levers)

**WHY:** BigQuery charges by **bytes scanned**. A query that scans a whole 2 TB table costs ~200× more than one that scans a 10 GB partition.
**Partitioning** physically splits a table by a column (usually a date). A `WHERE service_date = '2025-05-31'` then scans only that day.
**Clustering** sorts data within partitions by up to four columns (e.g., `provider_id`, `member_id`), so filters/joins on those columns prune even further.

> Rule of thumb: **partition by the date you filter on most**, **cluster by the high-cardinality columns you filter/join on most.**

## 9.4 Bronze: raw, append-only

```sql
-- sql/bronze/claims_bronze.sql
CREATE TABLE IF NOT EXISTS `acme-health-data-dev.bronze_claims.claims_raw`
(
  claim_id            STRING,
  member_id           STRING,
  provider_npi        STRING,
  service_date        STRING,      -- kept as STRING on purpose: raw fidelity
  icd10_code          STRING,
  cpt_code            STRING,
  billed_amount       STRING,      -- raw; may contain "$1,200.00"
  status_raw          STRING,
  source_file         STRING,
  _ingested_at        TIMESTAMP,
  _ingest_date        DATE
)
PARTITION BY _ingest_date
OPTIONS (description = "Raw claims exactly as received. Append-only. Do not edit.");
```

Loading Bronze is done by the DAG via `GCSToBigQueryOperator` with `WRITE_APPEND` and the ingestion columns added.

## 9.5 Silver: clean & conformed

```sql
-- sql/silver/claims_silver.sql
-- Idempotent: rebuilds the target partition for the run date.
DECLARE run_date DATE DEFAULT @run_date;

CREATE TABLE IF NOT EXISTS `acme-health-data-dev.silver.claims`
(
  claim_id        STRING NOT NULL,
  member_id       STRING NOT NULL,
  provider_npi    STRING,
  service_date    DATE,
  icd10_code      STRING,
  cpt_code        STRING,
  billed_amount   NUMERIC,
  status          STRING,          -- standardized: PAID/DENIED/PENDED/PARTIAL
  is_denied       BOOL,
  _ingest_date    DATE,
  _processed_at   TIMESTAMP
)
PARTITION BY service_date
CLUSTER BY provider_npi, member_id;

-- Replace only this run's slice (idempotent re-runs)
MERGE `acme-health-data-dev.silver.claims` T
USING (
  SELECT
    claim_id,
    member_id,
    provider_npi,
    SAFE.PARSE_DATE('%Y-%m-%d', service_date)                         AS service_date,
    UPPER(TRIM(icd10_code))                                           AS icd10_code,
    UPPER(TRIM(cpt_code))                                             AS cpt_code,
    SAFE_CAST(REGEXP_REPLACE(billed_amount, r'[^0-9.]', '') AS NUMERIC) AS billed_amount,
    CASE UPPER(TRIM(status_raw))
      WHEN 'P'  THEN 'PAID'
      WHEN 'D'  THEN 'DENIED'
      WHEN 'PD' THEN 'PENDED'
      ELSE 'UNKNOWN'
    END                                                              AS status,
    UPPER(TRIM(status_raw)) = 'D'                                    AS is_denied,
    _ingest_date,
    CURRENT_TIMESTAMP()                                              AS _processed_at,
    -- dedupe: keep latest ingested row per claim_id
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY _ingested_at DESC) AS rn
  FROM `acme-health-data-dev.bronze_claims.claims_raw`
  WHERE _ingest_date = run_date
) S
ON  T.claim_id = S.claim_id AND S.rn = 1
WHEN MATCHED THEN UPDATE SET
  member_id=S.member_id, provider_npi=S.provider_npi, service_date=S.service_date,
  icd10_code=S.icd10_code, cpt_code=S.cpt_code, billed_amount=S.billed_amount,
  status=S.status, is_denied=S.is_denied, _ingest_date=S._ingest_date, _processed_at=S._processed_at
WHEN NOT MATCHED AND S.rn = 1 THEN INSERT ROW;
```

This single statement demonstrates the Silver job's whole contract: **type-cast, standardize, clean money strings, deduplicate, and write idempotently.**

## 9.6 Gold: business marts & KPIs

```sql
-- sql/gold/denial_summary.sql  (Claims Analytics Platform)
CREATE OR REPLACE TABLE `acme-health-data-dev.gold.denial_summary`
PARTITION BY month
CLUSTER BY provider_npi AS
SELECT
  DATE_TRUNC(service_date, MONTH)                       AS month,
  provider_npi,
  COUNT(*)                                              AS total_claims,
  COUNTIF(is_denied)                                    AS denied_claims,
  SAFE_DIVIDE(COUNTIF(is_denied), COUNT(*))             AS denial_rate,
  SUM(billed_amount)                                    AS total_billed
FROM `acme-health-data-dev.silver.claims`
WHERE service_date IS NOT NULL
GROUP BY month, provider_npi;
```

```sql
-- sql/gold/kpi_executive.sql  (Executive KPI Platform — Medical Loss Ratio)
CREATE OR REPLACE TABLE `acme-health-data-dev.gold.kpi_executive`
PARTITION BY month AS
WITH paid AS (
  SELECT DATE_TRUNC(service_date, MONTH) AS month, SUM(billed_amount) AS claims_paid
  FROM `acme-health-data-dev.silver.claims`
  WHERE status = 'PAID' GROUP BY month
),
prem AS (
  SELECT DATE_TRUNC(payment_date, MONTH) AS month, SUM(amount) AS premiums_earned
  FROM `acme-health-data-dev.silver.payments`
  WHERE payment_type = 'PREMIUM' GROUP BY month
)
SELECT
  COALESCE(paid.month, prem.month)                          AS month,
  prem.premiums_earned,
  paid.claims_paid,
  SAFE_DIVIDE(paid.claims_paid, prem.premiums_earned)       AS medical_loss_ratio
FROM paid FULL OUTER JOIN prem USING (month);
```

```sql
-- sql/gold/fraud_scores.sql  (Fraud Detection Platform — heuristic baseline)
CREATE OR REPLACE TABLE `acme-health-data-dev.gold.fraud_scores`
PARTITION BY scored_date
CLUSTER BY provider_npi AS
WITH provider_stats AS (
  SELECT provider_npi,
         AVG(billed_amount) AS avg_billed,
         STDDEV(billed_amount) AS sd_billed
  FROM `acme-health-data-dev.silver.claims`
  GROUP BY provider_npi
),
member_freq AS (
  SELECT member_id, cpt_code, service_date, COUNT(*) AS same_proc_same_day
  FROM `acme-health-data-dev.silver.claims`
  GROUP BY member_id, cpt_code, service_date
)
SELECT
  c.claim_id, c.provider_npi, c.member_id, c.service_date, c.billed_amount,
  CURRENT_DATE() AS scored_date,
  -- simple additive risk score (0..100), replace with Vertex AI model later
  LEAST(100,
      IF(ps.sd_billed > 0 AND c.billed_amount > ps.avg_billed + 3*ps.sd_billed, 40, 0)  -- outlier amount
    + IF(mf.same_proc_same_day > 3, 35, 0)                                              -- duplicate-ish
    + IF(c.billed_amount > 50000, 25, 0)                                                -- high absolute
  ) AS fraud_score
FROM `acme-health-data-dev.silver.claims` c
JOIN provider_stats ps USING (provider_npi)
JOIN member_freq mf ON mf.member_id=c.member_id AND mf.cpt_code=c.cpt_code AND mf.service_date=c.service_date;
```

## 9.7 Streaming inserts into BigQuery

Two patterns:
- **BigQuery subscription (recommended, no code):** a Pub/Sub subscription that writes messages straight into a BQ table. Lowest latency, fully managed. (See §10.4.)
- **Storage Write API / `insertAll`:** when you need transformation before landing, a small consumer (Cloud Run/Functions or Dataflow) writes rows via the Storage Write API.

## 9.8 BigQuery cost & performance optimization checklist

- Always filter on the partition column; **never `SELECT *`** on wide tables — select only needed columns (columnar storage means unread columns are free).
- Partition by date, cluster by your top filter/join columns.
- Materialize expensive joins into Gold tables instead of recomputing per dashboard load.
- Use **BI Engine** / Looker's caching for dashboard queries.
- Set **maximum bytes billed** on jobs to fail-fast on runaway queries.
- Prefer scheduled **incremental** Gold rebuilds over full-table rebuilds where possible.
- Use **table expiration** on scratch/streaming-staging tables.


---

# 10. Pub/Sub & the Streaming / Real-Time Path

## 10.1 WHY / WHAT / HOW

**WHY:** Batch is fine for the nightly loss ratio, but fraud and operational alerts need to fire in seconds, not hours. You also want producers (claim intake systems) decoupled from consumers (your warehouse, your fraud scorer) so neither blocks the other.
**WHAT problem it solves:** asynchronous, durable, scalable messaging. Producers publish and move on; multiple consumers each get their own copy.
**WHAT Pub/Sub does internally:** a **topic** is a named channel; a **subscription** is a durable queue attached to a topic. Pub/Sub stores messages until acknowledged (at-least-once delivery), buffers bursts, and scales horizontally with no capacity planning.
**HOW enterprises use it:** one topic per event type, one subscription per consumer, dead-letter topics for poison messages, and schema enforcement so bad payloads are rejected at the edge.

## 10.2 Topology

```
claim-intake-service ──publish──> TOPIC: claim-events ──┬── SUB: claim-events-to-bq ──> BigQuery (bronze stream table)
                                                        └── SUB: claim-events-to-scorer ──> Cloud Run scorer
                                                                                              │ score >= 80
                                                                                              ▼
                                                                                  publish ──> TOPIC: fraud-alerts
                                                                                              │
                                                                          SUB: fraud-alerts-to-pager ──> PagerDuty / email
                              (poison) ──> TOPIC: claim-events-dlq
```

## 10.3 Create topics & subscriptions

```bash
# Topics
gcloud pubsub topics create claim-events
gcloud pubsub topics create fraud-alerts
gcloud pubsub topics create claim-events-dlq

# A subscription that delivers to a Cloud Run scorer (push), with DLQ + retry policy
gcloud pubsub subscriptions create claim-events-to-scorer \
  --topic=claim-events \
  --push-endpoint="https://scorer-xxxx-uc.a.run.app/score" \
  --dead-letter-topic=claim-events-dlq \
  --max-delivery-attempts=5 \
  --ack-deadline=30
```

`[SCREENSHOT: Pub/Sub → Topics. You should see claim-events, fraud-alerts, claim-events-dlq, each with a Subscriptions count. Click claim-events → Subscriptions tab to see claim-events-to-bq and claim-events-to-scorer with Delivery type (Push/Pull) and Ack deadline. Error case: "Topic not found" on subscription create → create the topic first.]`

## 10.4 BigQuery subscription (no-code streaming ingestion)

```bash
# First create the target streaming table (schema must match message schema)
bq mk --table acme-health-data-dev:bronze_claims.claim_events_stream \
  claim_id:STRING,member_id:STRING,provider_npi:STRING,billed_amount:FLOAT,event_ts:TIMESTAMP

# Subscription that writes directly into BigQuery
gcloud pubsub subscriptions create claim-events-to-bq \
  --topic=claim-events \
  --bigquery-table=acme-health-data-dev:bronze_claims.claim_events_stream \
  --use-table-schema
```

This is the modern, lowest-effort streaming path: events land in Bronze within seconds, no consumer code to run.

## 10.5 Producer (Python)

```python
# producers/claim_event_producer.py
import json, os
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
TOPIC = publisher.topic_path(os.environ["GCP_PROJECT"], "claim-events")

def publish_claim(event: dict) -> str:
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(TOPIC, data, source="claim-intake")
    return future.result()  # message id

if __name__ == "__main__":
    publish_claim({
        "claim_id": "CLM-1001", "member_id": "M-555",
        "provider_npi": "1234567890", "billed_amount": 82000.0,
        "event_ts": "2025-05-31T10:15:00Z"
    })
```

## 10.6 Real-time fraud scorer (Cloud Run, push consumer)

```python
# scorer/main.py  (deployed to Cloud Run; receives Pub/Sub push)
import base64, json, os
from flask import Flask, request
from google.cloud import pubsub_v1

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
ALERTS = publisher.topic_path(os.environ["GCP_PROJECT"], "fraud-alerts")

def score(event: dict) -> int:
    s = 0
    if event.get("billed_amount", 0) > 50000: s += 50
    # In production, call a Vertex AI endpoint or read provider baselines here
    return min(s, 100)

@app.route("/score", methods=["POST"])
def handle():
    envelope = request.get_json()
    msg = envelope["message"]
    event = json.loads(base64.b64decode(msg["data"]).decode("utf-8"))
    risk = score(event)
    if risk >= 80:
        publisher.publish(ALERTS, json.dumps({**event, "fraud_score": risk}).encode())
    return ("", 204)  # ack
```

`[SCREENSHOT: Cloud Run → service "scorer" → Metrics tab. You should see Request count, Request latency, and Container instance count rising when events flow. Logs tab shows each scored event. Error case: 401/403 on push → the Pub/Sub push SA needs roles/run.invoker on the service.]`


---

# 11. Pipeline Catalog — the 20 DAGs

Every DAG follows the same lifecycle (sense → validate → load Bronze → build Silver → build Gold → quality-gate → notify). The table is the contract; §12 implements the pattern in full so the rest are mechanical to write.

| # | DAG | Platform | Schedule | Source → Target | SLA | Enterprise use case |
|---|---|---|---|---|---|---|
| 1 | `claims_pipeline` | Claims Analytics | Daily 02:00 | GCS claims → bronze→silver→gold.denial_summary | 4h | Daily claims load & denial reporting |
| 2 | `fraud_detection_pipeline` | Fraud (batch) | Daily 03:30 | silver.claims → gold.fraud_scores | 2h | Nightly risk scoring of all claims |
| 3 | `payments_pipeline` | Payments Analytics | Daily 02:30 | GCS payments → silver.payments → gold | 3h | Premium collection & reconciliation |
| 4 | `member_enrollment_pipeline` | Enrollment | Daily 01:30 | GCS enrollment → silver.members | 3h | Membership snapshots, plan mix |
| 5 | `provider_analytics_pipeline` | Provider | Daily 04:00 | silver.claims+ref → gold.provider_cost | 3h | Provider cost/quality outliers |
| 6 | `hospital_claims_pipeline` | Claims | Daily 02:10 | GCS hospital file → bronze/silver | 4h | Institutional (UB-04) claims |
| 7 | `pharmacy_claims_pipeline` | Claims | Daily 02:20 | GCS pharmacy file → bronze/silver | 4h | Rx (NCPDP) claims |
| 8 | `streaming_claims_pipeline` | Streaming ETL | Continuous / 15-min micro-batch | Pub/Sub stream table → silver | 15m freshness | Near-real-time claim availability |
| 9 | `real_time_fraud_alert_pipeline` | Real-Time Fraud | Event-driven (sensor) | fraud-alerts → notify+gold | seconds | Page investigators on high-risk events |
| 10 | `executive_kpi_pipeline` | Executive KPI | Daily 05:00 | silver → gold.kpi_executive | 1h | MLR, revenue, fraud exposure |
| 11 | `customer_churn_pipeline` | Enrollment | Daily 05:30 | silver.members → gold.churn | 2h | Churn prediction inputs |
| 12 | `premium_analytics_pipeline` | Payments | Daily 03:00 | silver.payments → gold.premium | 2h | Premium trend & adequacy |
| 13 | `policy_renewal_pipeline` | Enrollment | Daily 06:00 | silver.members+ref → gold.renewals | 2h | Renewal pipeline & risk |
| 14 | `rejected_claims_pipeline` | Claims | Daily 04:30 | silver.claims (denied) → gold | 2h | Denial root-cause & appeals |
| 15 | `data_quality_pipeline` | Governance | After each load | all silver → gold.dq_results | 30m | Row counts, nulls, freshness gates |
| 16 | `pii_masking_pipeline` | Governance | Daily 01:00 | bronze → masked silver views | 1h | DLP-based PHI de-identification |
| 17 | `audit_pipeline` | Governance | Daily 07:00 | Cloud Logging → gold.audit | 1h | Who accessed what (compliance) |
| 18 | `compliance_pipeline` | Governance | Weekly | controls checks → report | 1d | HIPAA-style control attestation |
| 19 | `monitoring_pipeline` | Monitoring | Hourly | metrics → Cloud Monitoring | 15m | Freshness/SLA/cost custom metrics |
| 20 | `orchestration_master_pipeline` | Orchestration | Daily 01:00 | triggers the others in order | — | Single control DAG (ExternalTaskSensor / TriggerDagRun) |

---

# 12. Fully Implemented DAGs (5) + the Reusable Pattern

## 12.1 Shared helpers (`plugins/common/callbacks.py`)

```python
# plugins/common/callbacks.py
import logging
from airflow.providers.google.cloud.hooks.monitoring import ... # illustrative
from airflow.utils.email import send_email

def task_failure_alert(context):
    ti = context["task_instance"]
    msg = f"DAG {ti.dag_id} task {ti.task_id} failed on {context['ds']}. Log: {ti.log_url}"
    logging.error(msg)
    send_email(to=["data-oncall@acme-health.com"], subject=f"[AIRFLOW FAIL] {ti.dag_id}", html_content=msg)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": __import__("datetime").timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
    "email_on_failure": False,  # handled by callback
}
```

## 12.2 DAG 1 — `claims_pipeline` (the canonical batch pattern)

```python
# dags/batch/claims_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.empty import EmptyOperator

PROJECT = "acme-health-data-dev"
LANDING = "acme-health-landing-dev"

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="claims_pipeline",
    description="Daily claims: GCS -> Bronze -> Silver -> Gold(denial_summary)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["claims", "batch", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_claims_file",
        bucket=LANDING,
        object="claims/ingest_date={{ ds }}/hospital_claims_{{ ds_nodash }}.csv",
        mode="reschedule",          # free the worker slot while waiting
        poke_interval=300,
        timeout=60 * 60 * 3,
    )

    load_bronze = GCSToBigQueryOperator(
        task_id="load_bronze",
        bucket=LANDING,
        source_objects=["claims/ingest_date={{ ds }}/hospital_claims_{{ ds_nodash }}.csv"],
        destination_project_dataset_table=f"{PROJECT}.bronze_claims.claims_raw",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        time_partitioning={"type": "DAY", "field": "_ingest_date"},
        autodetect=False,
        schema_fields=[
            {"name": "claim_id", "type": "STRING"},
            {"name": "member_id", "type": "STRING"},
            {"name": "provider_npi", "type": "STRING"},
            {"name": "service_date", "type": "STRING"},
            {"name": "icd10_code", "type": "STRING"},
            {"name": "cpt_code", "type": "STRING"},
            {"name": "billed_amount", "type": "STRING"},
            {"name": "status_raw", "type": "STRING"},
        ],
    )

    build_silver = BigQueryInsertJobOperator(
        task_id="build_silver",
        configuration={
            "query": {
                "query": "{% include 'sql/silver/claims_silver.sql' %}",
                "useLegacySql": False,
                "queryParameters": [
                    {"name": "run_date", "parameterType": {"type": "DATE"},
                     "parameterValue": {"value": "{{ ds }}"}}
                ],
            }
        },
    )

    build_gold = BigQueryInsertJobOperator(
        task_id="build_gold_denial_summary",
        configuration={"query": {"query": "{% include 'sql/gold/denial_summary.sql' %}",
                                 "useLegacySql": False}},
    )

    quality_gate = BigQueryInsertJobOperator(
        task_id="quality_gate",
        configuration={"query": {
            "query": f"""
                SELECT IF(
                  (SELECT COUNT(*) FROM `{PROJECT}.silver.claims`
                   WHERE service_date = '{{{{ ds }}}}') > 0,
                  1, ERROR('Quality gate failed: zero silver rows for run date'))
            """,
            "useLegacySql": False}},
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_file >> load_bronze >> build_silver >> build_gold >> quality_gate >> end
```

`[SCREENSHOT: Airflow UI → DAGs list. You should see claims_pipeline with a toggle (paused/unpaused), recent run circles (dark green = success), and the schedule. Click the DAG → Graph view shows start → wait_for_claims_file → load_bronze → build_silver → build_gold_denial_summary → quality_gate → end, each box colored by state. Error case: a red box → click it → Logs to read the BigQuery error.]`

## 12.3 DAG 2 — `fraud_detection_pipeline` (batch scoring)

```python
# dags/fraud/fraud_detection_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="fraud_detection_pipeline",
    schedule_interval="30 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fraud", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["fraud", "batch"],
) as dag:

    # Don't score until claims_pipeline finished building silver
    wait_claims = ExternalTaskSensor(
        task_id="wait_for_claims_silver",
        external_dag_id="claims_pipeline",
        external_task_id="build_silver",
        mode="reschedule",
        timeout=60 * 60 * 2,
    )

    score = BigQueryInsertJobOperator(
        task_id="build_fraud_scores",
        configuration={"query": {"query": "{% include 'sql/gold/fraud_scores.sql' %}",
                                 "useLegacySql": False}},
    )

    publish_high_risk = BigQueryInsertJobOperator(
        task_id="export_high_risk_for_review",
        configuration={"query": {
            "query": """
              CREATE OR REPLACE TABLE `acme-health-data-dev.gold.fraud_review_queue` AS
              SELECT * FROM `acme-health-data-dev.gold.fraud_scores`
              WHERE fraud_score >= 80 AND scored_date = CURRENT_DATE()
            """, "useLegacySql": False}},
    )

    wait_claims >> score >> publish_high_risk
```

## 12.4 DAG 8 — `streaming_claims_pipeline` (15-min micro-batch from the stream table)

```python
# dags/streaming/streaming_claims_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="streaming_claims_pipeline",
    schedule_interval="*/15 * * * *",   # micro-batch
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "streaming", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["streaming", "near-real-time"],
) as dag:

    # MERGE newly-arrived streamed events (last 20 min) into silver
    merge = BigQueryInsertJobOperator(
        task_id="merge_stream_into_silver",
        configuration={"query": {
            "query": """
              MERGE `acme-health-data-dev.silver.claims` T
              USING (
                SELECT claim_id, member_id, provider_npi,
                       CAST(NULL AS DATE) AS service_date, NULL AS icd10_code, NULL AS cpt_code,
                       CAST(billed_amount AS NUMERIC) AS billed_amount,
                       'PENDED' AS status, FALSE AS is_denied,
                       CURRENT_DATE() AS _ingest_date, CURRENT_TIMESTAMP() AS _processed_at
                FROM `acme-health-data-dev.bronze_claims.claim_events_stream`
                WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE)
              ) S
              ON T.claim_id = S.claim_id
              WHEN NOT MATCHED THEN INSERT ROW
            """, "useLegacySql": False}},
    )
    merge
```

## 12.5 DAG 15 — `data_quality_pipeline` (the reusable quality gate)

```python
# dags/governance/data_quality_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

CHECKS = {
    "silver_claims_not_empty":
        "SELECT IF(COUNT(*) > 0, 1, ERROR('silver.claims empty')) FROM `acme-health-data-dev.silver.claims`",
    "no_null_claim_ids":
        "SELECT IF(COUNTIF(claim_id IS NULL)=0, 1, ERROR('null claim_id found')) FROM `acme-health-data-dev.silver.claims`",
    "billed_amount_nonnegative":
        "SELECT IF(COUNTIF(billed_amount < 0)=0, 1, ERROR('negative billed_amount')) FROM `acme-health-data-dev.silver.claims`",
    "freshness_24h":
        "SELECT IF(MAX(_processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR),1,ERROR('stale silver.claims')) FROM `acme-health-data-dev.silver.claims`",
}

with DAG(
    dag_id="data_quality_pipeline",
    schedule_interval=None,            # triggered by other DAGs / master
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "governance", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["governance", "quality"],
) as dag:
    prev = None
    for name, q in CHECKS.items():
        t = BigQueryInsertJobOperator(
            task_id=name,
            configuration={"query": {"query": q, "useLegacySql": False}},
        )
        if prev: prev >> t
        prev = t
```

## 12.6 DAG 20 — `orchestration_master_pipeline` (one control DAG)

```python
# dags/orchestration_master_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="orchestration_master_pipeline",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["orchestration", "master"],
) as dag:

    start = EmptyOperator(task_id="start")

    def trig(dag_id):
        return TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,     # block until child finishes
            poke_interval=60,
            reset_dag_run=True,
        )

    enroll   = trig("member_enrollment_pipeline")
    claims   = trig("claims_pipeline")
    payments = trig("payments_pipeline")
    fraud    = trig("fraud_detection_pipeline")
    quality  = trig("data_quality_pipeline")
    kpi      = trig("executive_kpi_pipeline")

    # ordering: dims first, then facts, then scoring/quality, then KPIs
    start >> enroll >> claims >> payments >> fraud >> quality >> kpi
```

## 12.7 How to write the remaining 14 DAGs

Each remaining DAG is a parametrization of the canonical pattern above. To create, say, `pharmacy_claims_pipeline`:
1. Copy `claims_pipeline.py`.
2. Change `dag_id`, `schedule_interval`, the GCS `object` glob, and the Bronze table to the pharmacy ones.
3. Point `build_silver`/`build_gold` at the pharmacy SQL files (same shape as §9.5/§9.6).
4. Add it to `orchestration_master_pipeline` in the right order.

Because the SQL templating and the operators are identical, the differences are configuration, not code. The catalog table in §11 is the spec for each.


---

# 13. Data Quality, PII Masking, Audit & Compliance

## 13.1 PII / PHI masking with Cloud DLP

**WHY:** Member names, SSNs, and member IDs tied to diagnoses are PHI. Most analysts never need the raw value — they need a stable token to join on.
**HOW:** the `pii_masking_pipeline` runs **Cloud DLP** de-identification (or BigQuery hashing for simple cases) to produce a masked Silver layer. Analysts query the masked layer; only a tiny privileged role can see raw values via **column-level security**.

```sql
-- Simple deterministic tokenization in BigQuery (good enough for joins, not reversible)
CREATE OR REPLACE VIEW `acme-health-data-dev.silver.members_masked` AS
SELECT
  TO_HEX(SHA256(member_id))                     AS member_token,  -- stable pseudonym
  TO_HEX(SHA256(ssn))                           AS ssn_token,
  -- generalize DOB to year for analytics
  EXTRACT(YEAR FROM dob)                         AS birth_year,
  plan_id, enrollment_status, state
FROM `acme-health-data-dev.silver.members`;
```

For richer needs (format-preserving encryption, redaction of free text), call the **DLP API** from a `PythonOperator` and apply `infoTypes` like `US_SOCIAL_SECURITY_NUMBER`, `PERSON_NAME`, `US_HEALTHCARE_NPI`.

**Column-level security:** attach a **policy tag** (Data Catalog taxonomy) to raw PHI columns; grant `fineGrainedReader` only to the compliance/analytics-privileged group. Everyone else's `SELECT` on that column returns an error or null.

## 13.2 Audit pipeline

`audit_pipeline` reads **Cloud Audit Logs** (data access logs exported to BigQuery via a log sink) and builds `gold.audit` — who queried which PHI table, when, and from where. This is the evidence auditors ask for.

```bash
# Export data-access logs to a BigQuery dataset
gcloud logging sinks create audit-to-bq \
  bigquery.googleapis.com/projects/acme-health-data-dev/datasets/gold \
  --log-filter='logName:"cloudaudit.googleapis.com%2Fdata_access"'
```

## 13.3 Compliance pipeline (HIPAA-style control checks)

`compliance_pipeline` runs weekly assertions and writes an attestation report: encryption at rest enabled (CMEK), no public buckets, no service account with `owner`, audit logging on for PHI datasets, retention policies present, and DLP masking applied. Each check is a SQL/`gcloud` assertion that fails loudly if a control regresses.

---

# 14. Looker Semantic & Dashboard Layer

## 14.1 WHY Looker sits on top of Gold

**WHY:** If every analyst writes their own "MLR" SQL, you get ten different MLRs. Looker defines each metric **once** in LookML; every dashboard and report inherits that single definition.
**WHAT it is:** a modeling + BI layer. **Views** describe tables (dimensions & measures), **Explores** join views, **Models** group explores against a connection, and **Dashboards** visualize Explores.
**HOW enterprises use it:** governed metrics, row-level security by member/region, scheduled report delivery, and drill-downs from a KPI to the underlying claims.

## 14.2 Connect Looker to BigQuery

1. **Looker Admin → Connections → New Connection.**
2. Dialect: **Google BigQuery Standard SQL**. Provide project `acme-health-data-dev`, the service-account JSON, and a temp dataset for PDTs.
3. Test → it should report "Can connect, Can run query, Can use temp dataset."

`[SCREENSHOT: Looker → Admin → Connections → connection test results. You should see a list of green checks (Connect, Kill, SQL Runner, Temp table, etc.). Error case: red "Can use temp dataset: false" → the SA lacks bigquery.dataEditor on the scratch dataset.]`

## 14.3 LookML examples

```lookml
# views/claims.view.lkml
view: claims {
  sql_table_name: `acme-health-data-dev.silver.claims` ;;

  dimension: claim_id { primary_key: yes; type: string; sql: ${TABLE}.claim_id ;; }
  dimension: provider_npi { type: string; sql: ${TABLE}.provider_npi ;; }
  dimension_group: service { type: time; timeframes: [date, week, month, year]; sql: ${TABLE}.service_date ;; }
  dimension: is_denied { type: yesno; sql: ${TABLE}.is_denied ;; }
  dimension: status { type: string; sql: ${TABLE}.status ;; }

  measure: claim_count { type: count }
  measure: total_billed { type: sum; sql: ${TABLE}.billed_amount ;; value_format_name: usd }
  measure: denied_count { type: count; filters: [is_denied: "yes"] }
  measure: denial_rate {
    type: number
    sql: SAFE_DIVIDE(${denied_count}, ${claim_count}) ;;
    value_format_name: percent_2
  }
}
```

```lookml
# models/healthcare.model.lkml
connection: "acme_bq"
explore: claims {
  label: "Claims Analytics"
  join: providers { sql_on: ${claims.provider_npi} = ${providers.npi} ;; relationship: many_to_one }
}
```

## 14.4 Dashboards to build

| Dashboard | Key tiles (dimensions × measures) | Filters / drill-downs |
|---|---|---|
| **Executive KPI** | MLR by month (line), revenue vs claims paid, fraud exposure, membership | Date range, plan, region; drill MLR → monthly claims |
| **Claims** | Denial rate by provider, top denial reasons, claims by status | Provider, date, status; drill to claim list |
| **Fraud** | High-risk claims (score ≥ 80), risk by provider, outlier amounts | Score threshold slider; drill to claim & provider history |
| **Payments** | Premium collected vs expected, aging, reconciliation gaps | Plan, billing cycle |
| **Real-time** | Streaming claim volume (last 60 min), live fraud alerts | Auto-refresh 1 min |

For each: add **calculated fields** (e.g., MLR = paid/premium), **filters** (date range, plan), **drill-downs** (provider → claim list), and **scheduled delivery** (e.g., the Executive dashboard emailed to leadership at 8 a.m.).

`[SCREENSHOT: Looker Executive KPI dashboard. You should see a top row of single-value tiles (MLR %, Premiums, Claims Paid, Members), a monthly MLR trend line below, and a provider denial-rate bar chart. A "Filters" bar at top has Date Range and Plan. Clicking a point on the MLR line drills into that month's claims. Error case: tile shows "Query execution failed" → open the tile's SQL via the three-dot menu → "Explore from here" to debug.]`

`[SCREENSHOT: Looker scheduled delivery dialog (dashboard → three-dot → Schedule delivery). You should see Destination (Email/Slack/GCS), Format (PDF/CSV), Frequency, and Filters to apply. Save and it appears under Admin → Schedules.]`


---

# 15. Security & HIPAA-style Controls

> This section describes engineering controls that *support* HIPAA-style compliance. Actual HIPAA compliance is a legal/organizational program (BAAs, risk assessments, workforce training) — not something a document or a config alone provides. Treat this as the technical layer of a broader program.

## 15.1 Identity & access

- **Service accounts per workload**, never human users, for pipelines (see §5.5).
- **Principle of least privilege:** custom roles in prod containing only the permissions tasks actually call.
- **No `owner`/`editor` on service accounts.** Audit IAM regularly with `gcloud projects get-iam-policy`.
- **Workload Identity** for GKE/Cloud Run so pods assume SAs without key files.
- **Avoid SA key files** where possible; if unavoidable, store in Secret Manager and rotate.

## 15.2 Network

- **Private IP Composer** and **private GKE** so nodes/metadata DB have no public IPs.
- **VPC Service Controls** perimeter around BigQuery/GCS/Pub/Sub to prevent data exfiltration to projects outside the perimeter.
- **Private Google Access** for egress to Google APIs without public internet.
- **IAP** in front of any admin UI.

## 15.3 Data protection

- **Encryption at rest** is on by default; for regulated data use **CMEK** (customer-managed keys in Cloud KMS) so you control key rotation and revocation.
- **Encryption in transit** via TLS everywhere (default for Google APIs).
- **PHI de-identification** via DLP + column-level policy tags (§13.1).
- **Bucket retention policies** (7-year) and **object versioning** for raw data immutability.

## 15.4 Secrets

- **Secret Manager** for DB passwords, API keys, SA keys. Airflow connections reference secrets via the Secret Manager backend; nothing sensitive lives in code or Variables.

```bash
echo -n "super-secret-pw" | gcloud secrets create cloudsql-pw --data-file=-
gcloud secrets add-iam-policy-binding cloudsql-pw \
  --member="serviceAccount:composer-runner@acme-health-data-dev.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

## 15.5 Audit

- **Data Access audit logs** enabled on PHI datasets, exported to BigQuery (§13.2), retained per policy.

---

# 16. Monitoring, Logging, Alerting & Incident Response

## 16.1 What to monitor (the four golden questions)

1. **Did it run?** — DAG/task success/failure.
2. **Did it run on time?** — SLA misses, scheduler lag.
3. **Is the data fresh & correct?** — freshness and quality-gate results.
4. **What is it costing?** — BigQuery bytes scanned, Composer environment cost.

## 16.2 Cloud Logging

Composer streams Airflow scheduler/worker/task logs to **Cloud Logging** and to the environment's GCS `/logs` folder. Useful filters:

```
resource.type="cloud_composer_environment"
severity>=ERROR
```

`[SCREENSHOT: Logs Explorer with the filter above. You should see ERROR-level log lines from the scheduler/worker, each expandable to a full stack trace with the DAG/task labels. Use "Create alert" from a query to turn a recurring error into an alerting policy.]`

## 16.3 Cloud Monitoring metrics & alerts

Composer exposes built-in metrics; the platform also publishes **custom metrics** from `monitoring_pipeline` (e.g., `silver_claims_freshness_minutes`). Build alerting policies:

| Alert | Condition | Notification |
|---|---|---|
| DAG failure | Composer metric `dag_run` failed > 0 in 1h | PagerDuty + email |
| SLA miss | task SLA exceeded | Email |
| Data stale | custom freshness metric > 90 min | PagerDuty |
| Cost spike | BigQuery scanned bytes/day > threshold | Email finance + lead |
| Scheduler heartbeat | scheduler healthy = false | PagerDuty |

```bash
# Example: alert on any Composer environment unhealthy (illustrative; tune in console)
gcloud monitoring policies create --policy-from-file=composer_health_policy.json
```

`[SCREENSHOT: Cloud Monitoring → Alerting → Create policy. You should see "Select a metric" (e.g., composer.googleapis.com/environment/healthy), a condition threshold, and Notification channels. After creation it appears in the Alerting list with state OK/Firing. Error case: no notification channel configured → create one under Notification channels first.]`

## 16.4 Retry & SLA architecture

- **Task-level retries** (`retries=3`, exponential `retry_delay`) absorb transient failures.
- **`sla`** per task triggers SLA-miss callbacks for chronic slowness.
- **Idempotent writes** (MERGE / partition truncate) make retries safe.
- **Dead-letter topics** (Pub/Sub) capture poison messages for the streaming path.

## 16.5 Incident response runbook (abbreviated)

1. Alert fires → on-call acknowledges in PagerDuty.
2. Open Airflow UI → identify failed DAG/task → read task logs.
3. Classify: data issue (bad source file), infra issue (worker OOM), or code bug.
4. Mitigate: re-drop/fix source file and clear the task; or scale workers; or roll back the DAG via CI/CD.
5. Backfill the affected partition; verify quality gate passes.
6. Write a postmortem; add a quality check that would have caught it.

---

# 17. CI/CD Architecture

**WHY:** DAGs are code. They need review, tests, and automated, auditable deployment — not someone dragging files into a bucket.
**Flow:**

```
Developer → Git PR → (Cloud Build CI) lint + pytest (DAG import test) → merge to main
        → (Cloud Build CD) deploy DAGs/plugins to the Composer DAGs bucket per env
```

```yaml
# ci/cloudbuild.yaml
steps:
  - name: python:3.11
    entrypoint: bash
    args:
      - -c
      - |
        pip install apache-airflow==2.9.1 pytest
        pytest tests/ -q              # includes a DAG-import smoke test

  - name: gcr.io/google.com/cloudsdktool/cloud-sdk
    entrypoint: bash
    args:
      - -c
      - |
        gcloud composer environments storage dags import \
          --environment acme-composer-dev --location us-central1 \
          --source dags/
options:
  logging: CLOUD_LOGGING_ONLY
```

```python
# tests/test_dag_integrity.py — fails the build if any DAG has an import error
from airflow.models import DagBag
def test_no_import_errors():
    db = DagBag(dag_folder="dags/", include_examples=False)
    assert not db.import_errors, db.import_errors
```

Promote dev → test → prod by pointing the deploy step at the next environment after approvals.

---

# 18. Cost Optimization, Budget Alerts & Cleanup

> **The single most important sentence in this document for a learner:** Cloud Composer bills continuously for as long as the environment exists, whether or not any DAG runs. If you are doing this as a learning/portfolio project, **delete the Composer environment when you stop working**, or it will quietly run up your bill.

## 18.1 What actually costs money (ranked)

1. **Cloud Composer environment** — always-on GKE + Cloud SQL metadata DB. *Biggest steady cost.*
2. **BigQuery query (analysis) bytes scanned** — spikes with bad queries (`SELECT *`, no partition filter).
3. **GKE / Cloud Run** custom services (the scorer) — scale-to-zero on Cloud Run helps.
4. **Cloud SQL** instances you provision for reference data.
5. **BigQuery storage** — cheap, but grows; use partition expiration.
6. **GCS storage** — cheap; lifecycle rules move/delete old data.
7. **Pub/Sub** — by volume; usually small.

## 18.2 Cost optimization levers

- **Stop/delete Composer when idle** (learning projects) — see teardown below.
- **Partition + cluster** every large table; **never `SELECT *`**.
- **Set maximum bytes billed** per query to cap accidents.
- **Cloud Run scale-to-zero** for the scorer.
- **Right-size** the Composer environment (Small in dev).
- **Lifecycle + expiration** on GCS and BigQuery scratch.
- **BI Engine / Looker caching** to avoid re-scanning for dashboards.
- **BigQuery editions / slot reservations** only when query volume justifies committed capacity.

## 18.3 Budget alerts

```bash
gcloud billing budgets create \
  --billing-account=XXXXXX-XXXXXX-XXXXXX \
  --display-name="health-data-dev-monthly" \
  --budget-amount=100USD \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0
```

`[SCREENSHOT: Billing → Budgets & alerts. You should see the budget with a progress bar of actual vs budgeted spend and the threshold rules (50/90/100%). Alerts email the billing admins when crossed. Note: budgets alert; they do NOT auto-stop spending.]`

## 18.4 Cleanup / teardown scripts

```bash
#!/usr/bin/env bash
# teardown.sh — STOP THE BILL. Run when you're done for the day/project.
set -euo pipefail
PROJECT=acme-health-data-dev
REGION=us-central1

# 1) The expensive one: delete the Composer environment (no "pause" exists)
gcloud composer environments delete acme-composer-dev --location=$REGION --quiet

# 2) Delete Cloud Run scorer
gcloud run services delete scorer --region=$REGION --quiet || true

# 3) Delete any Cloud SQL instance you created
# gcloud sql instances delete acme-ref-db --quiet || true

# 4) Empty + delete buckets (CAREFUL: this destroys data)
for b in acme-health-landing-dev acme-health-tmp-dev; do
  gcloud storage rm -r gs://$b/** 2>/dev/null || true
  gcloud storage buckets delete gs://$b --quiet || true
done

# 5) (Optional) delete BigQuery datasets
# for ds in bronze_claims silver gold ref; do bq rm -r -f -d $PROJECT:$ds; done

echo "Teardown complete. Verify in Billing that Composer is gone."
```

> There is **no "pause" for Composer** — to stop its cost you must **delete** the environment and recreate it later (≈20 min). Recreating is cheap; leaving it running is not. Keep your DAGs/SQL in Git so recreation is one CI run.

## 18.5 Free-tier guidance for learners

BigQuery and GCS have small monthly free allowances; Pub/Sub and Cloud Run have generous free tiers. **Composer does not have a meaningful free tier** — it's the line item to watch. Strategy: build and test DAGs locally where possible, spin Composer up only when you need the managed scheduler, and tear it down the same day.

---

# 19. Troubleshooting Guide

| Symptom | Likely cause | Fix |
|---|---|---|
| New DAG not showing in UI | Import error or not synced | Check "DAG Import Errors" banner; verify file landed in DAGs bucket; check DAG processor logs |
| Task stuck in `queued`/`scheduled` | No free worker slots / scheduler lag | Raise worker max; check `max_active_tasks`; scale scheduler |
| `403 PERMISSION_DENIED` from BigQuery | SA missing `jobUser`/`dataEditor` | Grant roles (§5.5) on the correct project |
| GCS sensor times out | File never arrived / wrong path glob | Verify upstream drop; check `{{ ds }}` vs `{{ ds_nodash }}` in object path |
| BigQuery "query too large / bytes billed exceeded" | Missing partition filter / `SELECT *` | Add `WHERE partition_col = ...`; select needed columns; set max bytes billed |
| Duplicate rows after retry | Non-idempotent load | Switch to MERGE / partition WRITE_TRUNCATE |
| Composer env stuck "Creating"/"Error" | API not enabled / SA missing role / network misconfig | Enable composer API; grant `composer.worker`; check VPC/subnet ranges |
| Pub/Sub push returns 401/403 | Push SA lacks invoker | Grant `roles/run.invoker` to the push SA |
| Looker tile "Query execution failed" | LookML/SQL error or perms | Open tile SQL → Explore; check connection temp-dataset perms |
| Surprise bill | Composer left running | Run `teardown.sh`; set budget alerts |
| Worker OOM-killed | Pandas processing large data in-worker | Push transform to BigQuery SQL; don't materialize big frames in the worker |

---

# 20. Scaling Strategies

- **Push compute to BigQuery.** Workers orchestrate; BigQuery crunches. This alone removes most scaling pain.
- **Partition/cluster** so query cost grows sub-linearly with data.
- **Composer worker autoscaling** + concurrency caps to handle backfills without melting the cluster.
- **Streaming via BQ subscriptions / Dataflow** scales horizontally with no capacity planning.
- **Separate environments** (dev/test/prod) so load is isolated.
- **Slot reservations** when query volume becomes predictable and large (commit capacity for a discount + stable performance).
- **Modularize SQL** into incremental Gold rebuilds rather than full refreshes as volumes grow.
- **Multi-region** considerations only when latency/residency demands it — adds cost and complexity.

---

# 21. Future Enhancements

- **ML fraud model on Vertex AI** replacing the heuristic score; feature store from Silver; online prediction behind the Cloud Run scorer.
- **dbt** for SQL transformation modeling, tests, and lineage docs over BigQuery.
- **Dataform** (native BigQuery transformation orchestration) as an alternative/complement to SQL-in-DAGs.
- **Great Expectations / Dataplex data quality** for richer, declarative quality rules.
- **Dataplex** for a unified data mesh, cataloging, and governance across domains.
- **Real-time dashboards** via BigQuery BI Engine + Looker with auto-refresh.
- **CDC ingestion** from operational databases via Datastream.
- **FinOps automation**: scheduled teardown of dev Composer overnight via a tiny always-cheap Cloud Scheduler + Cloud Function.

---

# 22. Resume / Project Description

**Short (resume bullet form):**

- Designed and built an enterprise healthcare-insurance data platform on GCP using Cloud Composer (Airflow), BigQuery, Pub/Sub, GCS, and Looker, implementing a Bronze/Silver/Gold medallion architecture across 20+ orchestrated pipelines.
- Engineered batch and near-real-time ETL: GCS-landed claims/payments/enrollment loaded to BigQuery via idempotent, partitioned/clustered transformations; streaming claim events ingested through Pub/Sub BigQuery subscriptions.
- Implemented a fraud-detection capability combining nightly BigQuery scoring with a real-time Pub/Sub → Cloud Run alerting path that flags high-risk claims within seconds.
- Built governance into the platform: Cloud DLP PHI de-identification, column-level security via policy tags, audit-log export, IAM least-privilege service accounts, and HIPAA-style control checks.
- Established observability and FinOps: Cloud Monitoring/Logging alerting on DAG failures, freshness SLAs, and cost spikes; partitioning/clustering and disciplined teardown to control spend; CI/CD via Cloud Build with DAG-integrity tests.
- Delivered governed analytics in Looker (LookML) including Executive MLR, Claims/Denial, Fraud, and Payments dashboards with drill-downs and scheduled delivery.

**Project summary (paragraph form):**

> Built a production-shaped data engineering platform for a healthcare payer on Google Cloud. Source claims, payments, and enrollment data land in Cloud Storage and flow through a BigQuery medallion architecture (Bronze→Silver→Gold) orchestrated by 20+ Cloud Composer (Airflow) DAGs. A Pub/Sub streaming path delivers near-real-time claim events and powers a real-time fraud-alerting service on Cloud Run. The platform enforces PHI protection (DLP, column-level security, least-privilege IAM), full observability (Cloud Monitoring/Logging, SLA and cost alerts), CI/CD (Cloud Build), and a governed Looker semantic layer exposing executive and operational dashboards.

---

# 23. Interview Questions & Answers

**Q1. What is Cloud Composer and what does it provision under the hood?**
Managed Apache Airflow. Creating an environment provisions a GKE cluster running the Airflow scheduler, workers, triggerer, web server, and DAG processor; a managed Cloud SQL instance as the metadata DB; and a GCS bucket holding `/dags`, `/plugins`, `/data`, `/logs`. You deploy DAGs by syncing files into the bucket.

**Q2. Explain the medallion architecture and why use three layers.**
Bronze stores raw data as-received (append-only, auditable, reprocessable). Silver is cleaned, typed, deduplicated, conformed, and PHI-controlled — the trusted working layer. Gold is business aggregates/marts that BI reads. Separation gives auditability, reproducibility, and a single presentation surface; you debug downward through layers when a KPI looks wrong.

**Q3. How do you control BigQuery cost?**
Partition by the date you filter on, cluster by high-cardinality filter/join columns, never `SELECT *`, set maximum bytes billed, materialize expensive joins into Gold, cache dashboard queries (BI Engine), and expire scratch tables.

**Q4. How do you make an Airflow task idempotent?**
Write deterministically: MERGE on a key, or WRITE_TRUNCATE a specific partition for the run date, so re-running produces the same result instead of duplicating rows. Use `{{ ds }}` to scope each run to its partition.

**Q5. Batch vs streaming here — when each?**
Batch for periodic, large, completeness-sensitive workloads (nightly MLR, denial reporting). Streaming for low-latency needs (real-time fraud alerts, near-real-time claim availability) via Pub/Sub + BQ subscriptions/Dataflow.

**Q6. How does Pub/Sub guarantee delivery and handle bad messages?**
At-least-once delivery: messages persist until acknowledged. Dead-letter topics capture messages that fail after N delivery attempts so the main flow isn't blocked by poison payloads.

**Q7. How do you protect PHI?**
Least-privilege IAM with per-workload service accounts, DLP de-identification/tokenization, column-level security via policy tags, CMEK encryption, private networking/VPC-SC, Secret Manager for credentials, and audit-log export for access tracking.

**Q8. The metadata DB — why does it matter?**
It's Airflow's brain: DAG/task state, XComs, connections, variables. If it's slow or unavailable, scheduling stalls platform-wide. In Composer it's a managed Cloud SQL instance.

**Q9. A DAG isn't appearing in the UI — debug it.**
Check the DAG Import Errors banner, confirm the file synced to the DAGs bucket, run the DAG-import unit test locally, and read the DAG processor logs for syntax/import errors.

**Q10. How do you orchestrate dependencies across DAGs?**
`ExternalTaskSensor` to wait on another DAG's task, or a master DAG using `TriggerDagRunOperator(wait_for_completion=True)` to sequence children (dims → facts → scoring → KPIs).

**Q11. How do you stop Composer from costing money in a learning project?**
There's no pause — delete the environment (keep DAGs in Git) and recreate when needed (~20 min). Set budget alerts. Composer is the dominant steady cost.

**Q12. Where would you add ML, and how?**
Replace the heuristic fraud score with a Vertex AI model: engineer features from Silver, train offline, deploy an endpoint, and call it from the Cloud Run scorer for real-time and from a batch DAG for nightly scoring.

---

# 24. Real Enterprise Use Cases

- **Nightly loss-ratio close.** Finance needs MLR by plan by 8 a.m.; the batch platform loads claims+payments overnight, builds `gold.kpi_executive`, and Looker emails the Executive dashboard at 8.
- **Provider fraud investigation.** The fraud platform surfaces a provider whose average billed amount is a 3-sigma outlier with repeated same-day duplicate procedures; investigators work the `fraud_review_queue`.
- **Real-time high-dollar claim alert.** A streamed claim event over $50k scores ≥80 and pages an investigator within seconds before payment is released.
- **Denial root-cause & appeals.** `rejected_claims_pipeline` clusters denials by reason and provider so the appeals team targets the highest-recovery categories.
- **Open-enrollment churn watch.** During enrollment season, the churn pipeline tracks lapsing members so retention teams can intervene.
- **Regulatory audit response.** When a regulator asks "who accessed member X's diagnoses last quarter," the audit pipeline answers from `gold.audit` in minutes.

---

# 25. Appendix: Folder Structures & Glossary

## 25.1 Repository structure
(See §5.6 — the canonical layout for DAGs, SQL, plugins, LookML, tests, and CI.)

## 25.2 Composer DAGs-bucket structure (managed)
```
gs://<composer-dags-bucket>/
├── dags/      ← your DAG .py files sync here
├── plugins/   ← custom operators/hooks
├── data/      ← shared files available to tasks
└── logs/      ← task logs (also in Cloud Logging)
```

## 25.3 Glossary
- **DAG** — Directed Acyclic Graph; an Airflow workflow.
- **Medallion** — Bronze/Silver/Gold layering pattern.
- **MLR** — Medical Loss Ratio = claims paid ÷ premiums earned.
- **PHI/PII** — Protected Health / Personally Identifiable Information.
- **NPI** — National Provider Identifier.
- **ICD-10 / CPT** — diagnosis / procedure coding standards.
- **CMEK** — Customer-Managed Encryption Keys.
- **DLP** — Data Loss Prevention (de-identification service).
- **SA** — Service Account (machine identity).
- **SLA** — Service-Level Agreement / objective on timeliness.
- **Idempotent** — re-running yields the same result.
- **Partition / Cluster** — BigQuery cost/performance physical organization.

---

*End of handbook v1.0. Each section can be expanded to full implementation depth on request — for example, fully writing all 20 DAG files, a complete LookML project, or a Vertex AI fraud-model module.*
