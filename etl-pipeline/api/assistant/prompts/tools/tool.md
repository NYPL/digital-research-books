
## Filters

<!-- based on https://turbopuffer.com/docs/query#filtering -->
<!-- Changes from TP docs include: changed "token" to "words", replace document with chunk
 -->

Filters refine search results. Think of them as a SQL WHERE clause.
The catalog uses a Turbopuffer search index, so use Turbopuffer filters syntax. 

### Filter Syntax

```
// Base Types
Condition  = [field, operation, value]
Filter     = Condition | And | Or | Not

// Collections
FilterList = [Filter, Filter, ...Filter]

// Logical Operators
And        = ["And", FilterList]
Or         = ["Or",  FilterList]
Not        = ["Not", Filter]
```

All fields are nullable (value = `null`).
<!-- null is a raw JSON null value, converted to python None in post-parsing -->

```
// Condition
"filters": ["publication_date", "Gte", "1900-01-01"]

// And
"filters": ["And", [
  ["author", "ContainsAnyToken", "Twain Hemingway"],
  ["publication_date", "Gte", "1900-01-01"]
]]

// Or
"filters": ["Or", [
  ["language", "Contains", "French"],
  ["language", "Contains", "Spanish"]
]]

// Not
"filters": ["Not", ["text", "ContainsAnyToken", "redacted censored"]]

// Nested (And > Or)
"filters": ["And", [
  ["publication_date", "Gte", "1800-01-01"],
  ["Or", [
    ["subject", "ContainsAnyToken", "Science Chemistry Physics"],
    ["author", "ContainsAllTokens", "Darwin"]
  ]]
]]
```

### Filter Operators

#### Equality Operators
- Equality operators on string and array of string fields are case sensitive.

`Eq` (value) - Exact match for field value. If value is null, this operation matches chunks missing the field.

`Contains` (value) - Checks whether the selected array field contains the provided value. Only use with array fields, not scalar fields.

`ContainsAny` (array[value]) - Checks whether the selected array field contains any of the values provided (intersection filter). Only use with array fields, not scalar fields.

#### Comparison Operators

`Lt` (value) - less-than 

`Lte` (value) -  less-than-or-equal 

`Gt` (value) - greater-than 

`Gte` (value) - greater-than-or-equal

#### Word Token Operators
- Word Token operators can only be used on string and array of strings typed fields.  
- All word token search operators are case insensitive.  <!-- This is because we indexed with case_sensitive=false -->
- Word Token operators break the query and indexed values into a sequence of word tokens to compare; "Tokens" in the operator names refers to words.

`ContainsAllTokens` (string) - Matches chunks where the field contains all the words present in the value string. If you need the words to be adjacent and in order, use ContainsTokenSequence instead. 

`ContainsTokenSequence` (string) - Matches chunks where the field contains all the words present in the value string, in the exact order and adjacent to each other. 

`ContainsAnyToken` (string) - Matches chunks where the field contains any of the words present in the value string. Requires that the field is configured for full-text search. Supports prefix queries in the same way as ContainsAllTokens.

<!-- excluded operators: NotEq, In, NotIn, NotContains, NotContainsAny, AnyLt, AnyLte, AnyGt, AnyGte, Glob, NotGlob, IGlob, NotIGlob, Regex -->

### Search Index Schema
Each chunk has the following allowed field names and allowed filter operations for each field:

<filter_schema>
  <field>
    <field_name>text</field_name>
    <data_type>string</data_type>
    <description>The full text of the text chunk.</description>
    <allowed_operations>ContainsAllTokens, ContainsTokenSequence, ContainsAnyToken</allowed_operations>
  </field>
  <field>
    <field_name>subject</field_name>
    <data_type>array of strings</data_type>
    <description>A list of subject strings associated with the book. Includes topic, genre, publication type, literary classification, etc.</description>
    <allowed_operations>ContainsAllTokens, ContainsTokenSequence, ContainsAnyToken</allowed_operations>
  </field>
  <field>
    <field_name>title</field_name>
    <data_type>string</data_type>
    <description>The title of the book the text chunk belongs to.</description>
    <allowed_operations>Eq, ContainsAllTokens, ContainsTokenSequence, ContainsAnyToken</allowed_operations>
  </field>
  <field>
    <field_name>author</field_name>
    <data_type>array of strings</data_type>
    <description>All authors who contributed to the book.</description>
    <allowed_operations>ContainsAllTokens, ContainsTokenSequence, ContainsAnyToken</allowed_operations>
  </field>
  <field>
    <field_name>language</field_name>
    <data_type>array of strings</data_type>
    <description>Languages the book is written in. ISO 639 full language names, case-sensitive, first letter uppercase (e.g. "English", "French").</description>
    <!-- see SFRRecordManager.getLanguage() -->
    <allowed_operations>Contains, ContainsAny</allowed_operations>
  </field>
  <field>
    <field_name>publication_date</field_name>
    <data_type>string (ISO 8601)</data_type>
    <description>The publication date of the book.</description>
    <allowed_operations>Eq, Lt, Lte, Gt, Gte</allowed_operations>
  </field>
</filter_schema>

<!-- Filters can also be applied to the id field, which refers to the document ID. -->

#### Subject Examples
Below is an example selection of subject strings (one per line). Each book has an array of subject strings associated with it: 
  Administrative law -- United States
  American fiction
  American poetry
  United States -- Periodicals
  Science -- Periodicals
  English literature
  Engineering -- Periodicals
  United States
  Chemistry -- Periodicals
  Books -- Reviews
  United States -- History -- Civil War, 1861-1865
  Economics
  United States -- Sources -- History
  Music -- Periodicals
  Encyclopedias and dictionaries
  Technology -- Periodicals
  Theology -- Periodicals
  France -- History -- Revolution, 1789-1799
  Government publications -- Periodicals -- United States
  Science
  Agriculture -- Periodicals
  United States -- Politics and government
  Political science
  Philosophy
  Voyages and travels
  Europe -- Description and travel
  Art -- Periodicals
  Natural history
  Automobiles -- Periodicals
  Architecture -- Periodicals
  United States -- Economic conditions
  Law reports, digests, etc -- Great Britain
  Church history
  Biography -- Dictionaries
  Astronomy
  History, Ancient
  German literature
  Archaeology -- Periodicals
  Insurance -- Periodicals
  French literature
  Law reports, digests, etc -- United States
  Theology
  English drama
  Natural history -- Periodicals
  English essays
  Mineral industries -- Periodicals
  Catholic Church -- Periodicals
  Greek letter societies -- Periodicals
  United States -- Statistics
  French drama
  Napoleon 1769-1821
  Geology
  Episcopal Church -- Sources -- History
  Industrial statistics -- United States
  Election districts -- Maps -- New York (State)
  New York (State) -- Maps
  Children's stories, American
  Italy
  Rare books
  German fiction
  Africa -- Periodicals -- Social conditions
  Lewis and Clark Expedition (1804-1806)
  Egypt -- History -- To 640 A.D
  Arithmetic -- Textbooks
  Arctic regions -- Discovery and exploration
  Hydraulics
  Clay industries -- Periodicals


## Ranking

The `ranking_query` is used to rank text chunks based on the semantic similarity of the text chunk content and the input query.
This semantic similarity calculation is done via semantic embedding of the the `ranking_query` and each candidate text chunk into semantic vectors. A vector similarity search is done to find the text chunks with the most relevant semantic content to the query.


## Usage Examples
<!-- MAYBE: move this into main system prompt -->

### `And`/`Or` syntax

`And` and `Or` always have exactly two elements: the operator name and a **single array containing all child conditions**. Every child condition must itself be an array.

❌ **Incorrect** — conditions passed as extra top-level elements instead of inside one array:
```json
{ "filters": ["And", ["publication_date", "Gte", "1900-01-01"], ["publication_date", "Lt", "2000-01-01"]] }
```

❌ **Incorrect** — nested array used instead of a flat array of conditions:
```json
{ "filters": ["And", [
    [["publication_date", "Gte", "1800-01-01"], ["publication_date", "Lt", "1900-01-01"]],
    ["language", "Contains", "French"]
]] }
```
- The two `publication_date` conditions are wrapped in their own inner array instead of being placed alongside `language` as siblings. This is not valid — each condition must be a direct element of the single wrapping array.
<!-- Interestingly, negative examples proved more effective in enforcing behavior for gemini flash 3 in testing -->

✅ **Correct** — all conditions wrapped together in one array:
```json
{ "filters": ["And", [
    ["publication_date", "Gte", "1900-01-01"],
    ["publication_date", "Lt", "2000-01-01"]
]] }
```
- Notice: The 2 "publication_date" filter conditions are contained in a wrapping array.


❌ **Incorrect** — single condition wrapped in `And`:
```json
{ "filters": ["And", ["publication_date", "Gte", "1900-01-01"]] }
```

✅ **Correct** — use the condition directly when there is only one:
```json
{ "filters": ["publication_date", "Gte", "1900-01-01"] }
```

### Nested `And` and `Or` filters

Using nested And and Or filters:

```
"filters": ["And", [
    ["publication_date", "Gte", "1900-01-01"],
    ["publication_date", "Lt", "2000-01-01"],
    ["Not", ["text", "ContainsAnyToken", "redacted censored"]],
    ["Or", [
        ["subject", "ContainsAnyToken", "American literature English literature"],
        ["author", "ContainsAnyToken", "Fitzgerald Hemingway Faulkner Steinbeck"],
    ]],
]]
```


### Keyword match

If a user is trying to search for information that matches a specific phrase or exact spelling, the `ranking_query` parameter alone will not help because it ranks chunks based on a representation of the semantic meaning of the text, but does not require that a specific phrase or spelling is present. In these cases, use a "String Operator" filter to require that all results match the exact keywords or phrases the user is interested in.

**Example:** User asks: "Find mentions of the Treaty of Versailles"

```json
{
  "ranking_query": "Treaty of Versailles World War I peace agreement",
  "filters": ["text", "ContainsTokenSequence", "Treaty of Versailles"]
}
```

This ensures results must contain the exact phrase "Treaty of Versailles" while ranking by semantic relevance.

### Negative filters

The semantic search does not work well with query stings that include negative filter logic. For example, if a user asks: "What information is there about ancient south american irrigation techniques from societies other than the inca?", a `ranking_query` like "ancient south american irrigation not from the inca civilization" will not act as a negative filter excluding all content that mentions the inca civilization, on the contrary the "not" will get washed out in the semantic embedding representation and content that discusses incan irrigation techniques will get ranked as high semantic relevance. 

In cases where you want exclude certain content, use a `Not` filter. Here is a good query for this example user question:

```json
{
  "ranking_query": "ancient south american irrigation techniques water management agriculture",
  "filters": ["Not", ["text", "ContainsAnyToken", "inca incan"]]
}
```

This query allows the powerful semantic search to identify relevant passages about ancient irrigation in south america while any content that specifically mentions the keywords "inca" or "incan" is excluded.


### `ranking_query` only searches the "text" field

The `ranking_query` only ranks semantic matches in the "text" field (which represents a chunk of a book's full text). Do not include content in the the `ranking_query` that is intended to match other fields, like publish_date or author's name. The `filters` can match based on other fields, so use the `filters` to search for content in those fields. 

**Example:** The user wants to know about "Uruguayan literature from the 20th century" 

The `ranking_query` should not include "from the 20th century" because the period when a text is written is metadata, not direct text content. Instead you can use a filter on the appropriate non-text field to build a search that matches the user's intent. 

```json
{
  "ranking_query": "Uruguayan literature authors literary movements writing culture",
  "filters": ["publication_date", "Gte", "1900-01-01"]
}
```

Note how the temporal constraint "20th century" is applied via a publication_date filter rather than included in the semantic `ranking_query`.

### Un-searchable queries

If the user asks to search for content that isn't available in any of the indexed fields, do not include the information in the constructed search arguments because the request is unsearchable in the current data model. Let the user know what part of their query is unsearchable, and continue to execute a search using whatever is searchable for the user's query

**Example**: The user asks for mystery books that were first published as paperbacks. Construct subject search for mystery books.

```json
{
  "ranking_query": "mystery detective crime suspense thriller investigation",
  "filters": ["subject", "ContainsAnyToken", "mystery detective novel fiction"]
}
```
In your response, let the user know that you are returning results for a search for mystery books, but that you do not have binding metadata available so you are not able to narrow the search to just mystery books that were first released as paperbacks.


**Example**: The user asks for books that were first published as paperbacks. Because the search index does not include binding information about the books, and the user did not include any other search criteria in their message, you cannot construct and execute a search. Let the user know you are not able to execute their search because you do not have binding metadata available, and provide examples of queries you would be able to fulfill.

**Example**: The user asks "What chapter of this book mentions slavery in the American south?" in the "ContentSearch" context. Chapter metadata is *not* available for the text chunks in the search index. Search for content related to slavery, but let the user know that you *do not* have chapter metadata available but that you *do* have page number metadata available for the text excerpts you provide.


### Avoid ambiguous semantic queries

Avoid Homonyms or ambiguous meaning in `ranking_query`. 

Ambiguous example (bad): "I'm looking for a history of the several stories that exist under Bryant Park for the library" 
- This is bad because stories could mean tales or floors of a building. This ranking_query would mix text chunks that match both meanings high in the rankings and do a bad job at isolating a specific meaning.

Clear example (good): "history of the many floors of underground rooms that exist under Bryant Park for the library" 
- Clearly indicates one meaning.

If the user's actual search intent is ambiguous, ask for clarification rather than create an ambiguous `ranking_query`.


### Compound phrases in filters

When filtering for compound phrases (multi-word terms that should be kept together), make sure to use `ContainsAllTokens` or `ContainsTokenSequence` instead of `ContainsAnyToken` since `ContainsAnyToken` matches if ANY of the words appear not the full compound phrase. This can lead to false matches.

**Example:** User asks for books about shipbuilding or naval architecture.

❌ **Incorrect approach:**
```json
{
  "ranking_query": "shipbuilding naval architecture ship construction",
  "filters": ["subject", "ContainsAnyToken", "Shipbuilding Ship-building Naval Architecture"]
}
```
This is problematic because `ContainsAnyToken` will match subjects that contain just "Architecture" (like "Gothic Architecture" or "Modern Architecture"), which are unrelated to naval topics.

✅ **Correct approach:**
```json
{
  "ranking_query": "shipbuilding naval architecture ship construction",
  "filters": ["Or", [
    ["subject", "ContainsAnyToken", "Shipbuilding Ship-building Naval"],
    ["subject", "ContainsAllTokens", "Naval Architecture"]
  ]]
}
```
This ensures "Naval Architecture" is treated as a compound phrase using `ContainsAllTokens`, so a subject must contain both "Naval" AND "Architecture" to match. Alternatively, you could use `ContainsTokenSequence` if the words must be adjacent and in order:

```json
{
  "ranking_query": "shipbuilding naval architecture ship construction",
  "filters": ["Or", [
    ["subject", "ContainsAnyToken", "Shipbuilding Ship-building"],
    ["subject", "ContainsTokenSequence", "Naval Architecture"]
  ]]
}
```

### Language filters

`language` is an array field. Use `Contains` for a single language or `ContainsAny` for multiple. Values are case-sensitive ISO 639 full names (e.g. `"Russian"`, `"English"`, `"French"`).

❌ **Incorrect** — `ContainsAnyToken` is not a supported operation on `language`:
```json
{ "filters": ["language", "ContainsAnyToken", "Russian"] }
```

✅ **Correct** — single language:
```json
{ "filters": ["language", "Contains", "Russian"] }
```

✅ **Correct** — multiple languages:
```json
{ "filters": ["language", "ContainsAny", ["Russian", "English"]] }
```

### Subject filters

The subject field includes both meta-descriptions of genre, literary classification, etc (ex: "Periodicals", "Literature", "Encyclopedias", etc..) and content descriptions (ex: "Theology", "Voyages", "Episcopal Church"). 

Our search tool is designed to search the full text of books in order to surface where relevant content exists even in unexpected books (For example a search for basket weaving can surface an econ textbook with a description basket weaving as an example of artisan industry). 

Use subject filters to filter for meta-descriptors (genre, literary classification, etc) and not content descriptors. Let the `ranking_query` handle surfacing books with the desired content. Don't add subject filters that are not requested by user.

**Example:** User asks about price elasticity of demand.

❌ **Incorrect approach:**
```json
{
  "ranking_query": "price elasticity of demand economic theory",
  "filters": ["subject", "ContainsAnyToken", "Economics"]
}
```
This unnecessarily restricts results to books classified as "Economics" when relevant content might exist in other types of books.

✅ **Correct approach:**
```json
{
  "ranking_query": "price elasticity of demand economic theory consumer behavior market response"
}
```
This lets the semantic search surface the most relevant content regardless of subject classification.




<!-- `Args` is parsed into the tool call argument definition JSON, the text above is parsed into the Description -->
<!-- MAYBE: just use the programmatic assignment of the FunctionTool rather than this parsing approach since its so bespoke anyway -->
Args:
  ranking_query: The query string used to rank text chunks in the search results. See the "Ranking" section of the tool description for more details.
   

  filters: (optional) The filter to apply to the query. See "Filtering" section of the tool description for syntax details. Remember, always wrap the conditions of an And/Or filter in a wrapping array (e.g. `["And", [["publication_date", "Gte", "1750-01-01"], ["publication_date", "Lt", "1790-01-01"]]]`) !!!!

  filters_match_null: (optional, defaults to True) When True, automatically modifies filters on potentially incomplete fields (subject, language, publication_date, author) to also match chunks where those fields are null. This ensures search results include books with incomplete metadata. For example, a filter like `["subject", "ContainsAnyToken", "poetry"]` is automatically transformed to `["Or", [["subject", "ContainsAnyToken", "poetry"], ["subject", "Eq", null]]]`. Set to False if you specifically want to exclude results with missing metadata for filtered fields.

<!-- 
    Args:
        ranking_query: The query string used to rank text chunks in the search results based on semantic similarity.
        filters: Optional filter specification to apply to the query. See tool documentation for syntax.
        filters_match_null: Optional boolean (default True) to include null-matching for incomplete fields.
 -->