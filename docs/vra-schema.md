# VRA Postgres Schema — Minimum Viable

Companion to `vra-system-design.md` and `vra-etl-dedup-design.md`. This is the **minimum set of tables and fields** the system actually needs, derived from inventorying what Turbopuffer documents carry and what the agent reads back. Anything not in this doc was deliberately dropped from the current production schema (see §3 "What we explicitly drop").

---

## 1. Core schema (already in `vra-system-design.md`)

These tables are the orchestration backbone — they don't change based on what fields the search layer needs.

```sql
CREATE TABLE vra_items (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    raw_manifest  JSONB NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, source_id)
);

CREATE TABLE vra_manifestations (
    id                BIGSERIAL PRIMARY KEY,
    cluster_key       TEXT NOT NULL UNIQUE,
    preferred_item_id BIGINT NOT NULL REFERENCES vra_items(id),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE vra_item_manifestations (
    item_id          BIGINT PRIMARY KEY REFERENCES vra_items(id) ON DELETE CASCADE,
    manifestation_id BIGINT NOT NULL REFERENCES vra_manifestations(id) ON DELETE CASCADE,
    assigned_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX vra_item_manifestations_manifestation_idx ON vra_item_manifestations(manifestation_id);

CREATE TABLE vra_indexing_configs (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    chunk_size      INT NOT NULL,
    chunk_overlap   INT NOT NULL,
    embedding_model TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE vra_indexing_results (
    item_id           BIGINT NOT NULL REFERENCES vra_items(id) ON DELETE CASCADE,
    config_id         INT    NOT NULL REFERENCES vra_indexing_configs(id),
    status            TEXT   NOT NULL,
    execution_arn     TEXT,
    s3_chunks_key     TEXT,
    s3_embeddings_key TEXT,
    chunk_count       INT,
    indexed_at        TIMESTAMPTZ,
    error             TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_id, config_id)
);
CREATE INDEX vra_results_status_idx ON vra_indexing_results(config_id, status);
```

---

## 2. Content schema (produced by `vra-etl`)

Five tables. Together they cover **every** field the agent reads from search results, every Turbopuffer attribute we upsert, and every value the LLM gets as item-level context.

```sql
-- One row per item; written by vra-etl.
CREATE TABLE vra_item_metadata (
    item_id          BIGINT PRIMARY KEY REFERENCES vra_items(id) ON DELETE CASCADE,
    title            TEXT NOT NULL,
    subtitle         TEXT,
    language         TEXT,                          
    publication_date DATE,                          
    publisher        TEXT,
    page_count       INT,
    rights_uri       TEXT,                          -- normalized; CC URL, rightsstatements.org URI, or public-domain marker
    rights_statement TEXT,                          -- human-readable; displayed verbatim, never parsed
    extracted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- N rows per item; one row per author/contributor.
CREATE TABLE vra_item_authors (
    item_id  BIGINT NOT NULL REFERENCES vra_items(id) ON DELETE CASCADE,
    name     TEXT NOT NULL,
    role     TEXT NOT NULL DEFAULT 'author',  -- author | editor | translator | illustrator | ...
    position INT  NOT NULL,                   -- 0-based ordering within an item
    PRIMARY KEY (item_id, position)
);
CREATE INDEX vra_item_authors_name_idx ON vra_item_authors(name);

-- N rows per item; one row per subject heading.
CREATE TABLE vra_item_subjects (
    item_id BIGINT NOT NULL REFERENCES vra_items(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    PRIMARY KEY (item_id, subject)
);
CREATE INDEX vra_item_subjects_subject_idx ON vra_item_subjects(subject);

-- N rows per item; one row per external identifier.
CREATE TABLE vra_item_identifiers (
    item_id BIGINT NOT NULL REFERENCES vra_items(id) ON DELETE CASCADE,
    scheme  TEXT NOT NULL,                   -- isbn10 | isbn13 | issn | oclc | lccn | doi | ...
    value   TEXT NOT NULL,                   -- normalized: no hyphens, lowercase where applicable
    PRIMARY KEY (item_id, scheme, value)
);
CREATE INDEX vra_item_identifiers_lookup_idx ON vra_item_identifiers(scheme, value);

-- One row per *structurally interesting* page. Pages with no row are implicitly 'body'.
-- Drives chunker boundaries, frontend section navigation, and quality-filtering
-- of missing/blank pages. Average ~20–40 rows per item (covers, frontmatter,
-- chapter starts, index, scattered missing/illustration).
CREATE TABLE vra_item_page_markers (
    item_id       BIGINT NOT NULL REFERENCES vra_items(id) ON DELETE CASCADE,
    scan_order    INT    NOT NULL,           -- 1-based scan sequence (METS @ORDER)
    printed_label TEXT,                       -- printed page number; nullable, may be roman/etc. (METS @ORDERLABEL)
    role          TEXT   NOT NULL CHECK (role IN (
                    'cover','frontmatter','toc','chapter_start',
                    'index','blank','missing','illustration','other')),
    flags         TEXT[] NOT NULL DEFAULT '{}',  -- subset of the same vocabulary; e.g. {'illustration'} on a chapter_start page
    PRIMARY KEY (item_id, scan_order)
);
CREATE INDEX vra_item_page_markers_role_idx ON vra_item_page_markers(item_id, role);
```

#### 2.0.1 Canonical page-role vocabulary

The `role` column is a **source-agnostic** enum. Each source (METS pageTag, HathiTrust pageType, IA scandata, etc.) gets a mapper in `vra-etl` that translates its raw vocabulary into one of these values. Adding a new source means writing a new mapper, not changing the schema.

| Role | Meaning | Consumer behavior |
|---|---|---|
| *(no row)* | Regular content page (`body`) | **Default** — chunk normally |
| `cover` | Front or back cover | Chunker skips; FE may use as thumbnail |
| `frontmatter` | Title page, copyright, dedication, preface | Chunker keeps with reduced weight |
| `toc` | Table of contents pages | Chunker skips (not prose); FE nav target |
| `chapter_start` | First page of a chapter / major section | **Chunk boundary**; FE nav target |
| `index` | Back-of-book index | Chunker skips (not prose) |
| `blank` | Intentionally blank | Skip |
| `missing` | Page absent from scan | Skip; gap marker for citation |
| `illustration` | Image-dominant page (plate, foldout) | Optional vision-model path; otherwise skip |
| `other` | Doesn't fit above | Passthrough; treated as `body` unless flagged |

**`body` is the implicit default for pages with no marker row.** Don't store `body` rows — across the corpus ~85–90% of pages are body, and storing them would inflate the table by an order of magnitude for no consumer benefit. The CHECK constraint enforces this by omitting `body` from the allowed values.

**Mappers may return "no role"** for source tags that don't represent structural information. This is the critical escape hatch: a source can tag every page with something (e.g. GRIN METS applies `UNTYPICAL_PAGE` to ~94% of pages in the sample corpus, `IMPLICIT_PAGE_NUMBER` to most others), and the mapper must recognize these as "tagged-but-not-structural" and produce no row. Otherwise the table degenerates back to one-row-per-page. The per-source mapping table lives in `vra-etl-dedup-design.md`; the schema only mandates that the output be one of the canonical roles or nothing.

A page marker has **exactly one** `role` (the dominant classification) and **zero or more** `flags` drawn from the same vocabulary (`body` is also disallowed as a flag value, by convention). A `CHAPTER_START` page that also contains an illustration is `role='chapter_start', flags={'illustration'}`.

**Unknown source values** map to `role='other'` with a warning logged by the source mapper. We never hard-fail an item on an unrecognized page tag; the source value is not retained in the row (re-derive by reprocessing if the mapping changes). Unknown ≠ uninteresting: if a tag is known-but-uninteresting (like `UNTYPICAL_PAGE`), the mapper drops it entirely instead of emitting `'other'`.

**Evolution:** if a new source introduces a meaningfully distinct page kind, add it as a new role here and update the `CHECK` constraint — Postgres allows this in a single transactional `ALTER TABLE`. Don't smuggle it in via `flags`.

### 2.1 Field provenance — where each field ends up

| Field | Postgres column | Turbopuffer attribute | LLM context | Display |
|---|---|---|---|---|
| Title | `vra_item_metadata.title` | ✓ (3× BM25 boost) | ✓ | ✓ |
| Subtitle | `vra_item_metadata.subtitle` |  |  | ✓ |
| Language | `vra_item_metadata.language` | ✓ (filter, array) | ✓ | ✓ |
| Publication date | `vra_item_metadata.publication_date` | ✓ (filter, range) | ✓ | ✓ |
| Publisher | `vra_item_metadata.publisher` |  | ✓ |  |
| Page count | `vra_item_metadata.page_count` |  |  | ✓ |
| Rights URI | `vra_item_metadata.rights_uri` | ✓ (filter) | ✓ | ✓ (badge / link) |
| Rights statement | `vra_item_metadata.rights_statement` |  |  | ✓ |
| Authors | `vra_item_authors.name[]` (aggregated) | ✓ (array, BM25) | ✓ | ✓ |
| Subjects | `vra_item_subjects.subject[]` (aggregated) | ✓ (array, 2× BM25 boost) | ✓ | ✓ |
| Identifiers | `vra_item_identifiers(scheme, value)` |  | ✓ (ISBN/DOI when present) | ✓ (outbound links) |
| Page markers (structural pages) | `vra_item_page_markers(scan_order, printed_label, role, flags)` |  |  | ✓ (section nav, chunker hints) |
| `manifestation_id` | derived via `vra_item_manifestations` join | ✓ (used for FE join) |  |  |
| `item_id` | from `vra_items.id` | ✓ |  |  |
| Chunk text | (lives in S3 chunks JSON) | ✓ (BM25-indexed) | ✓ | ✓ (excerpt) |
| `chunk_index`, `start_page`, `end_page` | (lives in S3 chunks JSON) | ✓ |  | ✓ (page range) |

That's the entire schema surface. Anything not in this table either lives in S3 (chunk text, vectors, page metadata) or is derived at query time.

### 2.2 Upsert pattern at `vra-upsert`

When `vra-upsert` writes a chunk to Turbopuffer, it aggregates the author + subject tables into arrays:

```sql
SELECT
    m.title,
    m.subtitle,
    m.language,
    m.publication_date,
    m.publisher,
    m.rights_uri,
    COALESCE(array_agg(DISTINCT a.name) FILTER (WHERE a.name IS NOT NULL), '{}') AS authors,
    COALESCE(array_agg(DISTINCT s.subject) FILTER (WHERE s.subject IS NOT NULL), '{}') AS subjects
FROM vra_item_metadata m
LEFT JOIN vra_item_authors a USING (item_id)
LEFT JOIN vra_item_subjects s USING (item_id)
WHERE m.item_id = :item_id
GROUP BY m.item_id;
```

One query, all the attribute data needed for every chunk of one item.

---

## 3. What we explicitly drop

These were all present in the current production schema. Each is dropped because nothing in the inventory of "what Turbopuffer carries" or "what the agent reads" justifies the schema cost.

- `barcode` — chunk tracking only; the doc_id already identifies a chunk uniquely.
- `edition_id` (and the Work/Edition split) — replaced by `vra_manifestations` (preference-only). No edition concept in v2.
- `edition`, `edition_statement`, `extent`, `publication_place`, `measurements`, `volume`, `series`, `series_position`, `medium`, `alt_titles` — none read by the agent or written to Turbopuffer.
- `summary` — not used by the agent's snippet path; the LLM gets per-chunk excerpts, not a stored summary.
- `table_of_contents` (as a stored bibliographic field) — empirically absent from the GRIN/Google-Books METS corpus (0/6 sampled files carry MARC 505). Section navigation is instead derived from `vra_item_page_markers` (page-role boundaries), not a stored narrative TOC.
- **Per-page rows for `body` pages.** Only structurally interesting pages get rows in `vra_item_page_markers`; absence implies `body`. Chunk-level `start_page`/`end_page` for citation lives in S3 chunks JSON, not Postgres.
- `dates` (multi-valued FRBR date list) — replaced by single `publication_date`. The "date of first publication vs this edition" distinction is gone with editions.
- `links` table — not used by the agent.
- MARC raw bytes — input to ETL, not an output. Read from S3 (via `raw_manifest`) at extraction time; not stored on `vra_item_metadata`.
- Separate `vra_authors` / `vra_subjects` dim tables — see §4 for why authors are denormalized per item.

If any of these come back, it should be because something specific in the agent or frontend started reading them — not "we always had this column."

---

## 4. Schema design — star, snowflake, and the alternatives

A few terms worth defining before justifying the choice.

### 4.1 Star schema

One central **fact** (in our case, the indexable thing — chunks, conceptually, or `vra_indexing_results` as the operational fact). Surrounding **dimension** tables hang off it via foreign keys. Dimensions are **denormalized** — they hold their attributes inline, even if those attributes repeat across rows.

In our world:
- Fact: `vra_indexing_results` (or chunks, if you squint)
- Dimensions: `vra_items`, `vra_item_metadata`, `vra_item_authors`, `vra_item_subjects`, `vra_item_identifiers`, `vra_item_page_markers`, `vra_manifestations`

Authors live as `(item_id, name, role, position)`. The string `"Mark Twain"` appears once per item by Twain. No central author table.

```
      vra_item_authors
              |
              v
vra_item_metadata <-- vra_items --> vra_item_manifestations --> vra_manifestations
              ^
              |
      vra_item_subjects
```

### 4.2 Snowflake schema

A star with dimensions **further normalized**. Repeated attributes get extracted into their own tables. Authors become two tables:

```sql
CREATE TABLE vra_authors (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,
    viaf_id TEXT
);

CREATE TABLE vra_item_authors (
    item_id   BIGINT REFERENCES vra_items(id) ON DELETE CASCADE,
    author_id BIGINT REFERENCES vra_authors(id),
    role      TEXT,
    position  INT,
    PRIMARY KEY (item_id, position)
);
```

"Mark Twain" appears once total, referenced by N item-author rows. Same for subjects, publishers, languages — anything reused gets its own table.

### 4.3 Galaxy / fact constellation

Multiple fact tables sharing dimensions. Useful in data warehouses where you track several business processes against the same entities (sales + returns + inventory all sharing `dim_product`).

For VRA there's effectively one fact (indexing results) and one entity type (items). No need.

### 4.4 OBT — One Big Table

Denormalize everything into a single wide table. One row per chunk with title, authors_array, subjects_array, page span, vector_key, etc. Common in OLAP / columnar stores (BigQuery, ClickHouse, DuckDB).

We sort of *do* this in Turbopuffer — every chunk document carries denormalized title/authors/subjects. But Turbopuffer is a search engine, not the source of truth. Postgres holds the normalized form and the upsert step does the denormalization on demand.

### 4.5 Document / EAV models

- **Document**: a single JSONB blob per item containing everything. Maximally flexible, terrible to query, hard to evolve safely.
- **EAV** (entity-attribute-value): one tall table `(item_id, attribute_name, value)`. Even worse — type loss, no constraints, queries become unreadable.

Neither is appropriate when fields are well-known.

### 4.6 Why star wins here

Five reasons, in order of importance:

1. **The query pattern is "fetch one item's data."** Every search hit triggers a join from chunk → item metadata → authors → subjects. With a star schema this is a fan-out join from one PK; with a snowflake it's the same plus an extra lookup per repeated attribute. Star wins on the dominant access pattern.

2. **We don't need author identity yet.** Snowflaking pays off when you want to *find all items by an author*. Today, neither the agent nor the frontend supports faceted-by-author search. If/when that lands, we add `vra_authors` with a nullable `author_id` FK on `vra_item_authors` — a clean, additive migration. Don't pay the cost UNTIl there's a benefit.

3. **Author name resolution is hard.** "Mark Twain" vs "Twain, Mark" vs "Samuel Clemens" is an entire problem domain (VIAF, LoC name authority, OCLC). Adding `vra_authors` without solving it means a polluted dim table with duplicates that hide the savings.

4. **Storage cost of denormalization is trivial.** ~1M items × ~3 authors × ~30 bytes = ~90 MB. A rounding error.

5. **Migration into snowflake is easy.** Migration out of snowflake (if it doesn't pan out) requires reconstructing the denormalized form. Asymmetric reversibility — start with the simpler model.

### 4.7 What we'd revisit if requirements change

| Future requirement | Schema change |
|---|---|
| Faceted "items by author X" search | Add `vra_authors` (snowflake authors) |
| VIAF / LoC authority resolution | Add `vra_authors.viaf_id`, populate during ETL |
| Subject hierarchy / parent-child | Add `vra_subjects` with self-FK |
| Multiple languages per item (multilingual) | Promote `vra_item_metadata.language` to `vra_item_languages(item_id, language)` |
| Publisher faceting / canonicalization | Add `vra_publishers`, FK from metadata |
| ISBN-based dedup matching | Use existing `vra_item_identifiers` in the dedup cluster_key |
| Per-chunk or per-chapter rights | Promote rights to `vra_item_rights(item_id, scope, rights_uri, ...)` |

All additive. None require touching existing tables in a non-backward-compatible way.

---

## 5. Open questions

- **`language` as scalar or array?** Currently scalar in this schema; Turbopuffer attribute is an array. Most items have one language; we promote to `["en"]` at upsert time. If multilingual items become common, snowflake `vra_item_languages` (see §4.7).
- **Subtitle in card / LLM context?** Currently in metadata but not in the Turbopuffer attribute table. Worth confirming the frontend wants it.
- **Tiebreak for duplicate authors per item.** If the same name appears twice in different roles (author + editor), they coexist (different `position`). If genuinely duplicated, ETL dedupes before insert.
- **Publication date precision.** `DATE` forces a day. Many items have only a year. Options: store year-only in a separate `publication_year INT`, or use `DATE` with `0001-01-01`-style fudging (don't do this), or use `daterange`. Lean: keep `publication_date DATE` for the actual date when known, plus `publication_year INT` for the common year-only case. Update if confirmed.
- **Rights normalization vocabulary.** `rights_uri` is most useful if every source maps to the same controlled vocabulary (CC URLs + rightsstatements.org URIs + a small set of internal sentinels like `public-domain`). ETL needs a per-source mapper. What goes in `rights_uri` when the source gives us nothing? `NULL` (unknown, treat as restricted) vs an explicit `unknown` sentinel. Lean: `NULL` + a `rights_uri IS NOT NULL` filter on display surfaces.
- **Per-country rights overrides.** GRIN METS carries `gbs:perCountryViewabilities` with `fullViewCountries` / `snippetViewCountries` / `metadataViewCountries` / `noViewCountries` lists alongside the `fallbackViewability` we store as `rights_uri`. Across 6 sampled files only one had a non-empty override list (snippet-only in `mx`). Options if compliance ever asks: (a) ignore — current plan; (b) add `rights_restricted_countries TEXT[]` on `vra_item_metadata` for the rare overrides; (c) full per-country normalization in a new table. Lean (a) until there's a concrete request. `byRightsholderRequest` (boolean on the same element) is similarly not stored; it could matter for defending against takedowns but doesn't drive any user-facing behavior.
- **Section-level rollup of `vra_item_page_markers`.** Marker rows give per-page granularity; a `vra_item_sections(item_id, section_order, role, start_scan, end_scan, start_printed_label)` materialized view (derived by running window functions over markers) may be worth adding once we know the FE navigation pattern. Deferred until there's a consumer.
