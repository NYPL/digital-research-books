# Ingestion, ETL, and Dedup — Design

Companion to `vra-system-design.md`. That doc treats ingestion, ETL, and dedup as black boxes with contracts. This one fills them in.

The three stages are tightly coupled by the data they hand off:

```
[ source bucket / API ]
          |
          v
  +---------------+      writes vra_items
  |   Ingestion   | ---> (source, source_id, raw_manifest, updated_at)
  +---------------+
          |
          v
  +---------------+      reads vra_items.raw_manifest + raw files
  |   vra-etl     | ---> writes vra_item_metadata + authors + subjects
  +---------------+      + identifiers + page_markers (5 content tables)
          |
          v
  +---------------+      reads vra_item_metadata + vra_manifestations/vra_item_manifestations
  |   vra-dedup   | ---> writes vra_item_manifestations + vra_manifestations.preferred_item_id
  +---------------+      emits {manifestation_id, is_preferred} into SF payload
```

Everything downstream of dedup (chunk, embed, pdf-gen, upsert) is covered in the system design doc and not repeated here.

---

## 1. Ingestion

### 1.1 Responsibility

Ingestion is whatever process discovers source content and lands it in `vra_items`. It is **not** part of the per-item Step Functions pipeline — it runs out-of-band, on its own schedule, per source.

One ingestion job per source (one for Gutenberg, one for HathiTrust, etc.). They share nothing except the `vra_items` write contract.

### 1.2 Contract on `vra_items` writes

Each ingestion job:

1. **Discovers** new or changed content in its source (S3 prefix listing, source API, periodic export, etc.).
2. **Constructs `raw_manifest`** — a JSONB blob that describes the raw inputs in whatever shape the source provides. The handlers downstream are responsible for understanding the shapes they care about.
3. **Upserts** into `vra_items` keyed on `(source, source_id)`:
   - On insert: row is created, `discovered_at = updated_at = now()`.
   - On update: only writes if `raw_manifest` actually differs from the current row. Bumps `updated_at = now()`. **Does not write if nothing changed** — that would trigger spurious reindexes.

That last point is the key contract: **`updated_at` is the change-detection signal**, so ingestion must not bump it gratuitously.

### 1.3 `raw_manifest` shapes (per source)

The manifest format is per-source. Each downstream handler dispatches on `raw_manifest.format`.

**Gutenberg** (already-clean text files):
```json
{
  "format": "gutenberg_v1",
  "bucket": "drb-raw",
  "key": "gutenberg/12345/12345-0.txt",
  "metadata_url": "https://www.gutenberg.org/ebooks/12345"
}
```

**HathiTrust** (page-image scans + OCR):
```json
{
  "format": "hathitrust_v1",
  "bucket": "drb-raw",
  "prefix": "hathitrust/ark+=13960=t1zd0/",
  "pages": [
    {"key": "...0001.txt", "page_number": 1},
    {"key": "...0002.txt", "page_number": 2}
  ],
  "marc_key": "hathitrust/ark+=13960=t1zd0/marc.xml"
}
```

**LoC / page-text sources** (no explicit manifest file in the bucket):
```json
{
  "format": "loc_pages_v1",
  "bucket": "drb-raw",
  "prefix": "loc/abc123/pages/"
}
```

For prefix-only manifests, downstream handlers list the prefix themselves when they run. Ingestion doesn't need to enumerate.

(TBD: fill in the actual sources we support and their manifest shapes.)

### 1.4 Where ingestion runs

- **Scheduled ECS task** per source, on whatever cadence makes sense (daily / weekly / on-demand).
- Out of scope for this doc: how the ingestion containers are built and deployed. They share the same base image as VRA handlers but they are not Step Functions states — they are independent ECS tasks.
- Failures in ingestion are observed via CloudWatch / ECS task logs; they do not touch `vra_indexing_results`.

---

## 2. `vra-etl`

### 2.1 Input

```json
{ "item_id": 98765, "config_id": 1, "item_updated_at": "..." }
```

Loads `vra_items.raw_manifest` from Postgres inside the handler.

### 2.2 Output

ETL writes to **five content tables** (see `vra-schema.md` §2). All writes for one item happen in a single transaction:

```sql
-- 1. Item metadata (one row, upserted)
INSERT INTO vra_item_metadata (
    item_id, title, subtitle, language, publication_date,
    publisher, page_count, rights_uri, rights_statement, extracted_at
) VALUES (...)
ON CONFLICT (item_id) DO UPDATE SET ...;

-- 2-5. Replace child rows (DELETE then INSERT keeps things simple; the
--      tables are small per-item and there's no ordering dependency)
DELETE FROM vra_item_authors      WHERE item_id = :item_id;
DELETE FROM vra_item_subjects     WHERE item_id = :item_id;
DELETE FROM vra_item_identifiers  WHERE item_id = :item_id;
DELETE FROM vra_item_page_markers WHERE item_id = :item_id;

INSERT INTO vra_item_authors      (item_id, name, role, position) VALUES ...;
INSERT INTO vra_item_subjects     (item_id, subject) VALUES ...;
INSERT INTO vra_item_identifiers  (item_id, scheme, value) VALUES ...;
INSERT INTO vra_item_page_markers (item_id, scan_order, printed_label, role, flags) VALUES ...;
```

State JSON is returned unchanged — ETL doesn't add fields to the payload.

### 2.3 Per-format extraction

ETL dispatches on `raw_manifest.format`. One extractor per format. Each extractor returns the same shape:

```python
@dataclass
class ExtractedItem:
    metadata:    ItemMetadata            # title, subtitle, language, publication_date, publisher, page_count, rights
    authors:     list[Author]            # (name, role, position)
    subjects:    list[str]
    identifiers: list[Identifier]        # (scheme, value), normalized
    page_markers: list[PageMarker]       # structural pages only; see §2.4
```

| Format | Title | Language | Pub date | Page count | Authors | Subjects | Identifiers | Rights | Page markers |
|---|---|---|---|---|---|---|---|---|---|
| `gutenberg_v1` | Gutenberg header / metadata URL | fasttext on first ~5kb | metadata API | `None` | header | LCSH from metadata API | gutenberg_id | PD-US sentinel | none |
| `hathitrust_v1` / `grin_v1` | MARC 245$a + $b | MARC 041 → 008/35-37 → fasttext | MARC 260$c / 264$c | METS PREMIS / MARC 300$a | MARC 100 + 700 + 710 | MARC 6xx | MARC 010, 020, 035 + `gbs:VolumeID` | METS `gbs:fallbackViewability` | METS pageTag (see §2.4) |
| `loc_pages_v1` | (TBD) | fasttext on first N pages | (TBD) | `len(listed_pages)` | (TBD) | (TBD) | (TBD) | (TBD) | (TBD) |

(Fill in remaining sources.)

**Language detection** is centralized: every extractor produces a candidate language string, and a final normalizer maps it to ISO 639-1. fasttext is the fallback when no metadata language is available.

**Rights normalization** is centralized too: each source's raw rights signal (METS `gbs:fallbackViewability`, Gutenberg license, etc.) goes through a mapper that returns a canonical `rights_uri` (CC URL, rightsstatements.org URI, or internal sentinel like `public-domain`) plus the human-readable `rights_statement`. When the source gives nothing, `rights_uri` is `NULL` (display surfaces filter on `rights_uri IS NOT NULL`).

### 2.4 Page-role normalization

The `vra_item_page_markers` table uses a canonical, source-agnostic role vocabulary (see `vra-schema.md` §2.0.1). Each source extractor includes a **page-role mapper** that translates the source's native page-classification vocabulary into one of those canonical roles — or returns `None` to omit the page (implicit `body`).

**The mapper's most important job is dropping noise.** Many sources tag virtually every content page with something. Across 6 sampled GRIN METS files, ~94% of pages carried `UNTYPICAL_PAGE`; another ~3% had `IMPLICIT_PAGE_NUMBER`. If the mapper emitted a row for every tagged page, the table would balloon from ~18M rows (markers only) to ~180M (one-per-page). The mapper must recognize known-but-uninteresting tags and drop them.

#### GRIN / Google-Books METS pageTag mapping

For `raw_manifest.format` of `hathitrust_v1` or `grin_v1`, the source vocabulary is the `gbs:pageTag` set referenced from `<METS:div TYPE="page" ADMID="...">`. Mapping:

| Source pageTag | Canonical role | Notes |
|---|---|---|
| `FRONT_COVER`, `BACK_COVER` | `cover` | |
| `TITLE`, `COPYRIGHT` | `frontmatter` | |
| `TABLE_OF_CONTENTS` | `toc` | |
| `CHAPTER_START`, `FIRST_CONTENT_CHAPTER_START` | `chapter_start` | |
| `INDEX` | `index` | |
| `BLANK` | `blank` | |
| `MISSING`, `MISSING_PAGE` | `missing` | |
| `FOLDOUT`, `ML_IMAGE_ON_PAGE` | `illustration` | |
| `IMAGE_ON_PAGE` | *(drop)* | Too noisy to be a row by itself; could become a `flags={'illustration'}` decoration on an adjacent role if needed |
| `UNTYPICAL_PAGE` | *(drop)* | Source's de facto "body" tag; ~94% of pages |
| `IMPLICIT_PAGE_NUMBER` | *(drop)* | Means "OCR inferred the printed page number" — not structural |
| `PAGE_TURNBACK` | *(drop)* | Scanner artifact |
| anything else | `other` + log warning | Per the schema's unknown-tag policy |

Multiple tags on a single page: METS lists them space-separated in `ADMID`. The mapper picks the **highest-priority** mapping among them (priority: cover > frontmatter > toc > chapter_start > index > illustration > blank > missing > other), and any remaining structural classifications go into `flags`.

`scan_order` comes from the METS `<div TYPE="page">` `@ORDER` attribute; `printed_label` from `@ORDERLABEL` (nullable).

#### Other sources

- **Gutenberg**: no structural metadata at all — emit no page markers. Chunker treats the whole text as body.
- **HathiTrust direct** (non-GRIN): METS uses different vocabulary (`pageType` not `pageTag`). TBD when we wire that source up; same mapper-pattern applies.
- **LoC pages**: TBD.

### 2.5 Failure modes

- **Corrupt / unreadable raw file** → raise `BadInputError` (non-retryable). SM catches → `RecordFailure`. Operator inspects.
- **S3 throttle / 5xx** → raise `TransientError`. ASL `Retry` handles it.
- **MARC parse failure** for HathiTrust → log warning, fall back to filename-based title, continue. Better to have a row with imperfect metadata than to fail the whole pipeline.
- **No extractable text** (scanned-only PDF with no OCR layer) → raise `BadInputError`. Downstream chunk/embed would produce nothing useful; better to fail loudly here.

### 2.6 Idempotency

Upsert keyed on `item_id`. Re-running ETL overwrites the metadata row and replaces all child rows (authors, subjects, identifiers, page markers) with the latest extraction. No history kept.

---

## 3. `vra-dedup`

### 3.1 Input

```json
{ "item_id": 98765, "config_id": 1, "item_updated_at": "..." }
```

Loads `vra_item_metadata` (just written by ETL) plus `vra_items` for source/source_id.

### 3.2 Output

Two Postgres writes + two state-payload fields:

1. `vra_item_manifestations` — upsert `(item_id, manifestation_id)` with `assigned_at = now()`.
2. `vra_manifestations.preferred_item_id` — recomputed for the affected manifestation.
3. Payload gains:
   ```json
   { "manifestation_id": 4321, "is_preferred": true }
   ```

`vra-upsert` later reads these to decide whether to write to Turbopuffer.

### 3.3 Algorithm (current)

The cluster decision is mostly Postgres-driven. Sketch:

```
1. Compute cluster_key from this item's metadata.
   cluster_key = hash(normalize(title), normalize(primary_author), publication_year_bucket)

   - normalize(): lowercase, strip punctuation, collapse whitespace, drop articles
   - publication_year_bucket: year rounded to nearest 5 to absorb data noise

2. Look up vra_manifestations WHERE cluster_key = :cluster_key.
   - Found: assign this item to that manifestation_id.
   - Not found: INSERT a new vra_manifestations row with this item as preferred_item_id (initial),
     get the new manifestation_id.

3. Recompute preferred_item_id for the manifestation:
   - SELECT i.id, i.source, i.discovered_at
     FROM vra_items i JOIN vra_item_manifestations im ON im.item_id = i.id
     WHERE im.manifestation_id = :manifestation_id
   - Pick the highest-priority source (see §3.5); tiebreak on discovered_at ascending.
   - UPDATE vra_manifestations.preferred_item_id if it changed.

4. Emit { manifestation_id, is_preferred = (preferred_item_id == this item_id) }.
```

(Replace this sketch with the actual code's behavior. Specifically TBD: the exact `cluster_key` inputs and whether there are external API calls — author normalization against VIAF or similar.)

### 3.4 External calls

(TBD — list any external lookups the current code makes, e.g. ISBN resolution, author authority lookup. Each one is a latency + failure surface to be aware of.)

### 3.5 Source priority

Hardcoded list in code, lowest-priority first to make tiebreaks obvious:

```python
SOURCE_PRIORITY = [
    "gutenberg",
    "doab",
    "hathitrust",
    "nypl",
    # ...
]
# higher index = preferred
```

Tiebreaker between items from the same source: `vra_items.discovered_at` ascending (older copy wins — assume the newer record is a duplicate ingestion of the same content).

(Fill in the actual ordered list of sources. Get sign-off from whoever owns content quality.)

### 3.6 Failure modes

- **Transient Postgres error** → raise `TransientError`. ASL retry.
- **External lookup timeout / 5xx** (if any) → raise `TransientError`. ASL retry.
- **Genuinely unclusterable item** (e.g. no title extracted) → assign to a singleton manifestation (cluster_key includes the item_id as a unique fallback). The item still gets exposed; it just won't merge with any other source.

### 3.7 Idempotency

Re-running dedup on the same item is safe:
- `cluster_key` is deterministic.
- `vra_item_manifestations` upsert keyed on `item_id`.
- `vra_manifestations.preferred_item_id` recomputation is idempotent: same inputs, same answer.

The interesting case is **re-running when *other* items in the cluster have changed**. That's covered too — dedup always recomputes preference from the current cluster membership, so if a new higher-priority sibling appeared since last run, this run flips the preferred copy.

### 3.8 What dedup *doesn't* do

- Does **not** delete items or manifestation rows.
- Does **not** merge two existing manifestations if it turns out they're the same. If two clusters were created separately and later look like duplicates, that's a re-clustering job (deferred — see system design §13).
- Does **not** modify `vra_item_metadata`.
- Does **not** touch Turbopuffer. The Turbopuffer swap happens in `vra-upsert`, which reads dedup's output.

---

## 4. Open questions

- **Cluster key inputs**: title + author + year, or something richer (publisher, ISBN, page count bucket)? Now that `vra_item_identifiers` exists, ISBN-13 is an obvious strong signal — if two items share a normalized ISBN-13 they should cluster regardless of title/author normalization noise. Proposal: try identifier-based clustering first (any shared `isbn13` / `oclc` / `lccn` value), fall back to the hashed (title, author, year-bucket) key. Needs a look at how the current code computes it.
- **Author normalization**: do we hit any external authority (VIAF, OCLC) or is this purely string normalization?
- **Source priority order**: the placeholder list in §3.5 needs the actual answer.
- **Ingestion deployment**: are ingestion jobs ECS scheduled tasks, EventBridge rules, manual CLI invocations? Probably mix.
- **Re-ingestion semantics**: when an ingestion job picks up that an existing item's manifest needs updating, it writes the new manifest + bumps `updated_at`. Should it *also* explicitly invalidate downstream artifacts (delete S3 chunks/embeddings)? Or trust the per-item SM to overwrite them on the next run? (Lean: trust the SM, don't pre-delete.)
- **`metadata-only` items**: METS `gbs:fallbackViewability="metadata-only"` items have no usable text. Should ETL raise `BadInputError` (skip them entirely) or write metadata but no page markers and let the chunker no-op? Lean: write metadata + identifiers + rights, skip chunk/embed downstream by checking `rights_uri` in `vra-chunk`.
- **`IMAGE_ON_PAGE` as a flag, not a drop**: currently the GRIN mapper drops `IMAGE_ON_PAGE` entirely. If we ever build a vision-model path, we'd want those pages enumerated. Revisit when the vision pipeline is on the roadmap.
