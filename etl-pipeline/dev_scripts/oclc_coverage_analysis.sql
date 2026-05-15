-- Analysis of records missing OCLC identifiers across 3 scopes.
-- Identifiers are stored as varchar[] with format 'value|type'.
-- A record "has OCLC" if any element of its identifiers array ends with '|oclc'.

-- 1. All records
WITH oclc_flag AS (
    SELECT id,
           EXISTS (
               SELECT 1 FROM unnest(identifiers) AS elem
               WHERE elem LIKE '%|oclc'
           ) AS has_oclc
    FROM records
)
SELECT
    COUNT(*) FILTER (WHERE NOT has_oclc) AS without_oclc,
    COUNT(*) AS total
FROM oclc_flag;

-- 2. GRIN records
WITH oclc_flag AS (
    SELECT id,
           EXISTS (
               SELECT 1 FROM unnest(identifiers) AS elem
               WHERE elem LIKE '%|oclc'
           ) AS has_oclc
    FROM records
    WHERE source = 'grin'
)
SELECT
    COUNT(*) FILTER (WHERE NOT has_oclc) AS without_oclc,
    COUNT(*) AS total
FROM oclc_flag;

-- 3. GRIN public domain records
WITH oclc_flag AS (
    SELECT r.id,
           EXISTS (
               SELECT 1 FROM unnest(r.identifiers) AS elem
               WHERE elem LIKE '%|oclc'
           ) AS has_oclc
    FROM records r
    WHERE r.source = 'grin'
      AND split_part(r.rights, '|', 2) IN ('public_domain', 'https://creativecommons.org/publicdomain/zero/1.0/')
)
SELECT
    COUNT(*) FILTER (WHERE NOT has_oclc) AS without_oclc,
    COUNT(*) AS total
FROM oclc_flag;
