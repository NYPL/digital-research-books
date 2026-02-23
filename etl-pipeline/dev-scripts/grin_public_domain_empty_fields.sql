-- SQL statements to count non-empty values for specific fields
-- in public domain, GRIN source editions

-- public domain GRIN editions
CREATE TEMP VIEW grin_public_domain_editions AS
SELECT DISTINCT e.* -- bc of incorrect multiple items per edition.. we have duplicate editions unless distinct
FROM editions e
JOIN items i ON i.edition_id = e.id
JOIN item_rights ir ON ir.item_id = i.id
JOIN rights r ON r.id = ir.rights_id
WHERE i.source = 'grin'
  AND r.license LIKE '%public_domain%';

-- display head of view
SELECT * FROM grin_public_domain_editions limit 1000;

-- Get total count of editions in the view
SELECT COUNT(*) as total_editions
FROM grin_public_domain_editions;

-- non-empty percentages
SELECT 
    ROUND(AVG(CASE WHEN e.languages IS NOT NULL AND e.languages::text != '[]' AND e.languages::text != 'null' AND jsonb_array_length(e.languages) > 0 THEN 1 ELSE 0 END) * 100, 2) as languages,
    ROUND(AVG(CASE WHEN w.subjects IS NOT NULL AND w.subjects::text != '[]' AND w.subjects::text != 'null' AND jsonb_array_length(w.subjects) > 0 THEN 1 ELSE 0 END) * 100, 2) as subjects,
    ROUND(AVG(CASE WHEN e.publication_date IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as publication_date,
    ROUND(AVG(CASE WHEN e.id IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as edition_id,
    ROUND(AVG(CASE WHEN e.title IS NOT NULL AND TRIM(e.title) != '' THEN 1 ELSE 0 END) * 100, 2) as title,
    ROUND(AVG(CASE WHEN e.table_of_contents IS NOT NULL AND TRIM(e.table_of_contents) != '' THEN 1 ELSE 0 END) * 100, 2) as table_of_contents,
    ROUND(AVG(CASE WHEN e.summary IS NOT NULL AND e.summary != '{}' THEN 1 ELSE 0 END) * 100, 2) as summary
FROM grin_public_domain_editions e
JOIN works w ON w.id = e.work_id;

-- Clean up (optional bc view is deleted when DB session ends)
DROP VIEW IF EXISTS grin_public_domain_editions;




SELECT e.* 
FROM editions e
JOIN items i ON i.edition_id = e.id
WHERE i.source = 'gutenberg'
	and e.summary is not null
limit 100;

select DISTINCT source from items;
