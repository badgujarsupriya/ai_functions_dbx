-- ============================================================================
-- food_delivery_reviews_pipeline — Lakeflow Spark Declarative Pipeline source
-- Food-delivery reviews: bronze -> silver -> gold, INCREMENTALLY.
-- Notebook 04_incremental_pipeline has the walkthrough & demo runbook.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BRONZE: incremental ingest from the watched volume folder.
-- Auto Loader tracks every file it has already read (a checkpoint) — each refresh
-- ingests only NEW files. Drop a new CSV into the folder, refresh, and only it arrives.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_delivery_reviews_st
COMMENT 'Raw multilingual food-delivery reviews, ingested incrementally from CSV files in the raw_reviews volume.'
AS
SELECT review_id, order_id, review_ts, city, country, language, rating,
       review_text, customer_email, expected_sentiment, expected_issue,
       _metadata.file_path AS source_file            -- which CSV each row came from
FROM   STREAM read_files(
         '/Volumes/workspace/food_delivery_demo/raw_reviews/',
         format => 'csv', header => 'true'
       );

-- ----------------------------------------------------------------------------
-- SILVER 1: clean + mask PII BEFORE any model call, with DQ expectations.
--   too_short    -> DROP the row (junk carries no signal) — same rule as notebook 02
--   valid_rating  -> WARN only: keeps the row, logs a metric (constraint stacking demo)
-- Note: row-level expectations cannot see already-ingested history, so cross-batch
-- exact duplicates are NOT dropped here — gold deduplicates them (row_number over
-- text_hash). Production alternative: dropDuplicates within a watermark (Python).
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_delivery_reviews_masked_st (
  CONSTRAINT too_short    EXPECT (length(trim(review_text_masked)) >= 5) ON VIOLATION DROP ROW,
  CONSTRAINT valid_rating EXPECT (rating BETWEEN 1 AND 5)
)
COMMENT 'Clean reviews: PII masked with ai_mask, email dropped, types fixed. Junk rows dropped by the too_short expectation.'
AS
SELECT review_id, order_id,
       CAST(review_ts AS TIMESTAMP) AS review_ts,    -- bronze stores raw strings; silver fixes types
       city, country, language, rating,
       sha2(review_text, 256) AS text_hash,            -- fingerprint for dedup + similarity
       ai_mask(review_text, ARRAY('person name', 'phone number', 'email address')) AS review_text_masked
FROM   STREAM(bronze_delivery_reviews_st);

-- ----------------------------------------------------------------------------
-- SILVER 2: THE AI step — the same 6 functions as notebook 02, now INCREMENTAL:
-- the flow processes only NEW rows, so refresh 2 pays AI for only the 15 new reviews.
-- Inner subquery: translate, sentiment, classify, extract (on masked text).
-- Outer: summarize + fix grammar reuse the already-translated review_en — nothing is
-- translated twice.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_delivery_reviews_enriched_st
COMMENT 'AI-enriched reviews: English text, polished grammar, 15-word summary, sentiment, issue type, food item. AI runs once per new row.'
AS
SELECT b.*,
       ai_summarize(b.review_en, 15)  AS review_summary,     -- short digest for dashboards
       ai_fix_grammar(b.review_en)    AS review_en_polished -- clean text, customer-facing
FROM (
  SELECT review_id, order_id, review_ts, city, country, language, rating, text_hash,
         review_text_masked,
         ai_translate(review_text_masked, 'en')   AS review_en,
         ai_analyze_sentiment(review_text_masked) AS sentiment,
         ai_classify(review_text_masked,
           ARRAY('delivery_delay','food_quality','wrong_or_missing_item','price','app_issue','none')) AS issue_type,
         ai_extract(review_text_masked, '["food_item"]', MAP('version','2.1'))
           :response.food_item.value::STRING AS food_item
  FROM   STREAM(silver_delivery_reviews_masked_st)
) b;

-- ----------------------------------------------------------------------------
-- GOLD 1: business insights — recomputed from silver on every refresh (batch MV),
-- ZERO AI calls. row_number over text_hash deduplicates exact duplicates ACROSS
-- batches — the cross-batch duplicate that expectations could not see is caught here.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_delivery_issue_trend_mv
COMMENT 'Issue counts and average rating by country, deduplicated across batches. Zero AI calls at refresh.'
AS
WITH deduped AS (
  SELECT *, row_number() OVER (PARTITION BY text_hash ORDER BY review_id) AS rn
  FROM   silver_delivery_reviews_enriched_st
)
SELECT country, issue_type, count(*) AS review_count, round(avg(rating), 2) AS avg_rating
FROM   deduped
WHERE  rn = 1
GROUP  BY ALL;

-- ----------------------------------------------------------------------------
-- GOLD 2a: the escalation queue TARGET — a streaming table, so drafts are written ONCE
-- and never re-paid on later reads or refreshes (a materialized view would re-run
-- ai_gen on every refresh; a plain VIEW would re-pay on every read).
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE gold_support_escalation_queue_st (
  review_id INT, country STRING, issue_type STRING, rating INT,
  review_en STRING, draft_reply STRING
)
COMMENT 'Worst reviews with AI-drafted apology replies. Each review is drafted exactly once.';

-- ----------------------------------------------------------------------------
-- GOLD 2b: draft replies for NEW negative reviews only.
-- Streaming has no LIMIT 5 across batches (notebook 02 shows the batch-style cap);
-- the WHERE filter is the cost cap here.
-- The flow's checkpoint guarantees each source row is drafted exactly once —
-- refresh 2 drafts only the new reviews, never re-pays the first batch.
-- ----------------------------------------------------------------------------
CREATE FLOW draft_replies_flow
AS INSERT INTO gold_support_escalation_queue_st BY NAME
SELECT e.review_id, e.country, e.issue_type, e.rating, e.review_en,
       ai_gen('Write a polite 2-sentence apology to a food-delivery customer. '
              || 'Their problem: ' || e.issue_type || '. '
              || 'Their feedback: ' || e.review_en || ' '
              || 'Do not promise a refund or compensation.') AS draft_reply  -- guardrail in the prompt
FROM   STREAM(silver_delivery_reviews_enriched_st) e
WHERE  e.sentiment = 'negative' AND e.rating <= 2;

-- ----------------------------------------------------------------------------
-- GOLD 3: is the AI any good? Compare the AI output against the hand-labeled ground
-- truth carried in bronze (expected_sentiment / expected_issue). Zero AI calls.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_ai_quality_mv
COMMENT 'AI accuracy vs hand-labeled ground truth.'
AS
SELECT count(*) AS reviews_checked,
       round(avg(CASE WHEN e.sentiment = b.expected_sentiment THEN 1.0 ELSE 0 END) * 100, 1) AS sentiment_accuracy_pct,
       round(avg(CASE WHEN e.issue_type  = b.expected_issue    THEN 1.0 ELSE 0 END) * 100, 1) AS issue_accuracy_pct
FROM   silver_delivery_reviews_enriched_st e
JOIN   bronze_delivery_reviews_st b USING (review_id);