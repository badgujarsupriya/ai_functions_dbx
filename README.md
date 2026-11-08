# Your Pipeline Just Got a Brain: AI Functions in Databricks SQL

Demo material for **Session 1** of a three-part Databricks series — turn raw, multilingual customer reviews into structured, governed insights using nothing but SQL AI Functions.

| Session | Topic |
| --- | --- |
| **1 — this repo** | Your Pipeline Just Got a Brain: AI Functions in Databricks SQL |
| 2 — Dec 11, 2026 | Talk to Your Data: Building an AI-Powered Data Product (Genie) |
| 3 | Dashboards as Code: DevOps Meets AI/BI |

## What the demo shows

* **From unstructured text to insights** — 25 synthetic food-delivery reviews in 6 languages (English, Hindi, Spanish, German, Japanese, Portuguese), including PII, junk, and duplicate rows
* **AI-powered SQL** — translate, classify, score sentiment, and extract key details with built-in AI Functions, in a single query
* **Medallion architecture** — bronze (raw) → silver (masked + AI-enriched) → gold (business-ready)
* **Governed data** — everything lands in Unity Catalog tables with comments and tags (Genie-ready for Session 2)
* **Practical techniques** — prompt design, cost control, and quality checks built into the pipeline

## Repository contents

| File | Purpose | Runtime |
| --- | --- | --- |
| `notebooks/01_ai_functions_101` | Warm-up: what AI Functions are + signature and a one-line example of the 10 core functions | ~2 min |
| `notebooks/02_reviews_pipeline` | Main demo: the medallion pipeline (bronze → silver → gold) with quality checks and cost control | ~5–10 min |
| `README.md` | This file | — |
| `.gitignore` | Keeps local noise out of Git | — |

## Prerequisites

* A Databricks workspace (the **free edition** is enough)
* **Serverless** SQL warehouse or serverless compute — auto-selected, no cluster setup
* Permission to create a schema in your workspace catalog (`workspace.food_delivery_demo`)

## How to run

1. Clone this repo into your Databricks workspace (or import the notebooks).
2. Open `notebooks/01_ai_functions_101` → **Run All** (~2 minutes; single-string examples, a few cents).
3. Open `notebooks/02_reviews_pipeline` → **Run All** (~5–10 minutes).
4. Optional: clean everything up with the last cell of notebook 02 (`DROP SCHEMA ... CASCADE`).

## The dataset

25 synthetic reviews of a food-delivery app, spread across 6 languages:

| Language | Rows |
| --- | --- |
| English | 7 |
| Hindi | 4 |
| Spanish | 4 |
| German | 4 |
| Japanese | 3 |
| Portuguese | 3 |

Deliberately includes: 1 too-short review, 1 duplicate, and 2 reviews containing phone numbers/names (PII) — so the data-quality and masking steps have something to catch.

## Cost control

* Every AI function call invokes a model behind the scenes.
* Notebook 01 calls functions on **single literals** (a few cents total).
* Notebook 02 processes 23 clean rows **once** into tables (~100 small calls); all downstream queries are pure SQL.
* The optional `ai_query` section is **not run** by default.

## References

* [AI Functions in Databricks](https://docs.databricks.com/aws/en/large-language-models/ai-functions)
* [SQL built-in functions reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin)
* [ai_query function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)