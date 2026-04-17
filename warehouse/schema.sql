-- ============================================================
-- warehouse/schema.sql
-- Egyptian Job Market Analytics Pipeline — Star Schema
--
-- Fact table  : fact_job_postings
-- Dimensions  : dim_company, dim_location, dim_skill,
--               dim_experience, dim_date
-- Bridge      : bridge_job_skill  (job ↔ skills many-to-many)
--
-- Run once:
--   docker exec -it postgres psql -U airflow -d airflow 
-- ============================================================

-- ── Create dedicated schema ───────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS job_market;

SET search_path TO job_market;

-- ── dim_date ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    week            SMALLINT    NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,   -- 0=Monday … 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

-- ── dim_company ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_company (
    company_id      SERIAL PRIMARY KEY,
    company_name    VARCHAR(255) NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ── dim_location ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    location_raw    VARCHAR(255),
    country         VARCHAR(100) DEFAULT 'Egypt',
    UNIQUE (city, location_raw)
);

-- ── dim_experience ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_experience (
    experience_id   SERIAL PRIMARY KEY,
    label           VARCHAR(100)  NOT NULL UNIQUE,
    min_years       SMALLINT,
    max_years       SMALLINT,
    level           VARCHAR(20)  -- 'entry', 'mid', 'senior', 'lead'
);

-- ── dim_skill ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_skill (
    skill_id        SERIAL PRIMARY KEY,
    skill_name      VARCHAR(100) NOT NULL UNIQUE,   -- lowercase normalised
    category        VARCHAR(50)  -- 'language', 'framework', 'cloud', 'tool', etc.
);

-- ── fact_job_postings ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_job_postings (
    posting_id      SERIAL PRIMARY KEY,

    -- Natural key from source
    job_id          VARCHAR(255) NOT NULL UNIQUE,

    -- Foreign keys to dimensions
    company_id      INT REFERENCES dim_company(company_id),
    location_id     INT REFERENCES dim_location(location_id),
    experience_id   INT REFERENCES dim_experience(experience_id),
    scraped_date_id INT REFERENCES dim_date(date_id),

    -- Job attributes
    title           VARCHAR(255) NOT NULL,
    job_type        VARCHAR(50),
    keyword         VARCHAR(50),
    skills_count    SMALLINT    DEFAULT 0,
    days_ago        SMALLINT,

    -- Raw / audit fields
    url             TEXT,
    posted_date_raw VARCHAR(100),
    location_raw    VARCHAR(255),
    scraped_at      TIMESTAMPTZ,
    cleaned_at      TIMESTAMPTZ,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),

    -- Source tracking
    pipeline_run_id VARCHAR(100)   -- Airflow run_id for lineage
);

-- ── bridge_job_skill ──────────────────────────────────────────────────────────
-- Many-to-many: one job posting can require many skills
CREATE TABLE IF NOT EXISTS bridge_job_skill (
    posting_id  INT NOT NULL REFERENCES fact_job_postings(posting_id) ON DELETE CASCADE,
    skill_id    INT NOT NULL REFERENCES dim_skill(skill_id),
    PRIMARY KEY (posting_id, skill_id)
);

-- ── Indexes for dashboard query performance ───────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_company    ON fact_job_postings(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_location   ON fact_job_postings(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_experience ON fact_job_postings(experience_id);
CREATE INDEX IF NOT EXISTS idx_fact_date       ON fact_job_postings(scraped_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_keyword    ON fact_job_postings(keyword);
CREATE INDEX IF NOT EXISTS idx_fact_title      ON fact_job_postings(title);
CREATE INDEX IF NOT EXISTS idx_bridge_skill    ON bridge_job_skill(skill_id);

-- ── Pre-populate dim_experience ───────────────────────────────────────────────
INSERT INTO dim_experience (label, min_years, max_years, level) VALUES
    ('Not specified',  NULL, NULL, 'unknown'),
    ('0-1 years',      0,    1,    'entry'),
    ('0-2 years',      0,    2,    'entry'),
    ('0-3 years',      0,    3,    'entry'),
    ('1 years',        1,    1,    'entry'),
    ('1-2 years',      1,    2,    'entry'),
    ('1-3 years',      1,    3,    'entry'),
    ('1+ years',       1,    NULL, 'entry'),
    ('2 years',        2,    2,    'entry'),
    ('2-3 years',      2,    3,    'mid'),
    ('2-4 years',      2,    4,    'mid'),
    ('2-5 years',      2,    5,    'mid'),
    ('2+ years',       2,    NULL, 'mid'),
    ('3 years',        3,    3,    'mid'),
    ('3-5 years',      3,    5,    'mid'),
    ('3-7 years',      3,    7,    'mid'),
    ('3+ years',       3,    NULL, 'mid'),
    ('4 years',        4,    4,    'mid'),
    ('4-6 years',      4,    6,    'mid'),
    ('4+ years',       4,    NULL, 'mid'),
    ('5 years',        5,    5,    'senior'),
    ('5-7 years',      5,    7,    'senior'),
    ('5-10 years',     5,    10,   'senior'),
    ('5+ years',       5,    NULL, 'senior'),
    ('6 years',        6,    6,    'senior'),
    ('6-8 years',      6,    8,    'senior'),
    ('6+ years',       6,    NULL, 'senior'),
    ('7 years',        7,    7,    'senior'),
    ('1-7 years',      1,    7,    'mid'),
    ('8+ years',       8,    NULL, 'lead'),
    ('10+ years',      10,   NULL, 'lead')
ON CONFLICT (label) DO NOTHING;

-- ── Useful views for the BI dashboard ─────────────────────────────────────────

-- Top skills across all postings
CREATE OR REPLACE VIEW vw_top_skills AS
SELECT
    s.skill_name,
    COUNT(b.posting_id)  AS job_count,
    ROUND(COUNT(b.posting_id) * 100.0 / NULLIF(SUM(COUNT(b.posting_id)) OVER (), 0), 2) AS pct
FROM bridge_job_skill b
JOIN dim_skill s ON b.skill_id = s.skill_id
GROUP BY s.skill_name
ORDER BY job_count DESC;

-- Jobs by city
CREATE OR REPLACE VIEW vw_jobs_by_city AS
SELECT
    l.city,
    COUNT(f.posting_id)  AS job_count
FROM fact_job_postings f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.city
ORDER BY job_count DESC;

-- Jobs by experience level
CREATE OR REPLACE VIEW vw_jobs_by_experience AS
SELECT
    e.level,
    e.label,
    e.min_years,
    COUNT(f.posting_id) AS job_count
FROM fact_job_postings f
JOIN dim_experience e ON f.experience_id = e.experience_id
GROUP BY e.level, e.label ,e.min_years
ORDER BY e.min_years NULLS LAST;

-- Top hiring companies
CREATE OR REPLACE VIEW vw_top_companies AS
SELECT
    c.company_name,
    COUNT(f.posting_id) AS job_count
FROM fact_job_postings f
JOIN dim_company c ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY job_count DESC;

-- Daily scraping trend
CREATE OR REPLACE VIEW vw_daily_trend AS
SELECT
    d.full_date,
    d.day_name,
    COUNT(f.posting_id) AS jobs_scraped
FROM fact_job_postings f
JOIN dim_date d ON f.scraped_date_id = d.date_id
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date;
