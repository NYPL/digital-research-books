-- ============================================================
-- GRIN 10k Sample — SQL Queries Used to Construct 300 Barcodes
-- 270 from 9 diverse subjects (30 each) + 30 with no subject
-- Database: dcdw_production (prod-readonly)
-- Tables: grin_public_domain_10k, records
-- Output: newline-delimited barcode-only file (barcodes_300.txt)
-- ============================================================

-- -------------------------------------------------------
-- STEP 1: Discover all subjects and their barcode counts
-- Used to identify candidate subjects for diversity selection
-- -------------------------------------------------------
SELECT
    split_part(unnested_subject, '|', 1) AS subject_heading,
    COUNT(DISTINCT g.barcode) AS barcode_count
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id,
     unnest(r.subjects) AS unnested_subject
WHERE split_part(unnested_subject, '|', 1) <> ''
GROUP BY subject_heading
ORDER BY barcode_count DESC
LIMIT 60;

-- -------------------------------------------------------
-- STEP 2: Per-subject queries — 30 barcodes each (270 total)
-- Subject values match the first pipe-delimited segment of
-- records.subjects array elements (format: "heading||" or
-- "heading -- subdivision||authority|controlno")
-- -------------------------------------------------------

-- 1. English fiction (prose literary narrative)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'English fiction'
)
LIMIT 30;

-- 2. English poetry (verse / literary)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'English poetry'
)
LIMIT 30;

-- 3. Administrative law -- United States (legal / government documents)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'Administrative law -- United States'
)
LIMIT 30;

-- 4. Science -- Periodicals (scientific serials)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'Science -- Periodicals'
)
LIMIT 30;

-- 5. Engineering -- Periodicals (technical serials)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'Engineering -- Periodicals'
)
LIMIT 30;

-- 6. United States -- Periodicals (general US news / current affairs periodicals)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'United States -- Periodicals'
)
LIMIT 30;

-- 7. History -- Periodicals (historical serials)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'History -- Periodicals'
)
LIMIT 30;

-- 8. Encyclopedias and dictionaries (reference works)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'Encyclopedias and dictionaries'
)
LIMIT 30;

-- 9. French fiction (continental European literary prose)
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE EXISTS (
    SELECT 1 FROM unnest(r.subjects) s WHERE split_part(s, '|', 1) = 'French fiction'
)
LIMIT 30;

-- -------------------------------------------------------
-- STEP 3: No-subject barcodes — 30 barcodes (10% of 300)
-- Matches records with NULL, empty array, or zero-length
-- subjects array
-- -------------------------------------------------------
SELECT g.barcode
FROM grin_public_domain_10k g
JOIN records r ON r.id = g.record_id
WHERE r.subjects IS NULL
   OR r.subjects = '{}'
   OR array_length(r.subjects, 1) = 0
LIMIT 30;
