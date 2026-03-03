We refer to each text chunk in the search index as a "ChunkDocument".

## ChunkDocument Attributes

  text: 
    Data type: string
    Description: The full text of the text chunk.

  subject:
    Data type: array of strings
    Description: A list of subject strings that are associated with the book the text chunk belongs to. Subject strings include information like topic, genre, publication type, literary classification, etc. Below is an example selection of subject strings (one per line). Remember each book can have multiple subject strings associated with it: 
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

  publication_date
    Data type: string (iso 8601 formatted date)
    Description: The publication date of the book the text chunk belongs to.

  author
    Data Type: array of strings
    Description: An array of all the authors who contributed to the book the text chunk belongs to.

  title
    Data Type: string
    Description: The title of the book the text chunk belongs to.  

  language
    Data Type: array of string
    Description: Languages that the book is written in. array of ISO 639 full language names. Case Sensitive. First letter uppercase for each language name.
    <!-- see SFRRecordManager.getLanguage() -->



## Filtering

<!-- based on https://turbopuffer.com/docs/query#filtering -->
<!-- Changes to TP docs: changed "token" to "words" -->

Exact filters to apply to ChunkDocument attributes to refine search results. Think of it as a SQL WHERE clause.

Filters allow you to narrow down results by applying exact conditions to the returned ChunkDocument attributes. Conditions are arrays with an attribute name, operation, and value, for example:

  `["title", "Eq", "The Great Gatsby"]`
  `["author", "ContainsAnyToken", "Shakespeare Milton"]`
  `["publication_date", "NotEq", null]`

Values must have the same data type as the ChunkDocument attribute the filter is applied to, or an array of that type for operators like `ContainsAny`.

All attributes are nullable (value = `null`)
<!-- null must be represented as raw JSON null value, as all parameters are constructed by the agent as valid JSON -->

Conditions can be combined using `{And,Or}` operations:
```
// basic `And` condition
"filters": ["And", [
  ["author", "ContainsAnyToken", "Twain Hemingway"],
  ["publication_date", "Gte", "1900-01-01"]
]]

// conditions can be nested
"filters": ["And", [
  ["publication_date", "Gte", "1800-01-01"],
  ["Or", [
    ["subject", "ContainsAnyToken", "Science Chemistry Physics"],
    ["author", "ContainsAllTokens", "Darwin"]
  ]]
]]
```
<!-- Filters can also be applied to the id field, which refers to the document ID. -->

### Filtering Parameters

#### Equality Operators
- Equality operators on string and array of string attributes are case sensitive.

**Supported Attributes**: publication_date, title, author, language

`Eq` (value) - Exact match for attributes values. If value is null, matches ChunkDocuments missing the attribute. Only use with scalar attributes, not array attributes.

<!-- 
`NotEq` (value) - Inverse of Eq, for attributes values. If value is `null`, matches ChunkDocuments with the attribute.
 -->
<!-- Removing to reduce complexity bc this can be handled with `Or` filter combination. "author" or "language" is the only field were I think this might be used -->
<!-- `In` (array[value]) - Matches any attributes values contained in the provided list.

`NotIn` (array[value]) - Inverse of In, matches any attributes values not contained in the provided list. -->

<!-- #### Array Attribute Operators -->

`Contains` (value) - Checks whether the selected array attribute contains the provided value. Only use with array attributes, not scalar attributes.

<!-- `NotContains` (value) - Inverse of Contains. -->

`ContainsAny` (array[value]) - Checks whether the selected array attribute contains any of the values provided (intersection filter). Only use with array attributes, not scalar attributes.

<!-- `NotContainsAny` (array[value]) - Inverse of ContainsAny. -->

#### Comparison Operators

**Supported Attributes**: publication_date

`Lt` (value) - less-than 

`Lte` (value) -  less-than-or-equal 

`Gt` (value) - greater-than 

`Gte` (value) - greater-than-or-equal

<!-- #### Array Attribute Comparison Operators

`AnyLt` (value) - Checks whether any element of an array attribute is less than the provided value, using the same rules as Lt.

`AnyLte` (value) - Checks whether any element of an array attribute is less than or equal to the provided value, using the same rules as Lte.

`AnyGt` (value) - Checks whether any element of an array attribute is greater than the provided value, using the same rules as Gt.

`AnyGte` (value) - Checks whether any element of an array attribute is greater than or equal to the provided value, using the same rules as Gte. -->

<!-- `Glob` (globset) - Unix-style glob match against string or []string attribute values. The full syntax is described in the globset documentation. Glob patterns with a concrete prefix like "foo*" internally compile to efficient range queries, while patterns without a concrete prefix (e.g., "*foo*" or "*foo") will perform a full scan of the namespace.

`NotGlob` (globset) - Inverse of Glob, Unix-style glob filters against string or []string attribute values. The full syntax is described in the globset documentation.

`IGlob` (globset) - Case insensitive version of Glob.

`NotIGlob` (globset) - Case insensitive version of NotGlob.

`Regex` (string) - Regular expression match against string attribute values. Requires the regex schema attribute to be enabled before use. Warning: Doesn't support certain advanced features (e.g. look-around, backreferences). Currently requires exhaustive evaluation; not recommended for large namespaces or ANN queries unless used in conjunction with other selective filters. Contact us if you run into performance problems. -->

#### Word Token Operators
- Word Token operators can only be used on string and array of strings typed attributes.  
- All word token search operators are case insensitive.  
- Word Token operators break the filter value into a sequence of word tokens; "Tokens" in the operator names refers to words.

**Supported Attributes**: text, subject, title

`ContainsAllTokens` (string) - Matches ChunkDocuments that contain all the words present in the filter value string. If you need the words to be adjacent and in order, use ContainsTokenSequence instead. 

`ContainsTokenSequence` (string) - Matches ChunkDocuments that contain all the words present in the filter value string, in the exact order and adjacent to each other. 

`ContainsAnyToken` (string) - Matches ChunkDocuments that contain any of the tokens present in the filter input string. Requires that the attribute is configured for full-text search. Supports prefix queries in the same way as ContainsAllTokens.

#### Meta Operators (combine and negate filters)

`And` (array[filter]) - Matches if all of the filters match.

`Or` (array[filter]) - Matches if at least one of the filters matches.

`Not` (filter) - Matches if the filter does not match.


Complex Example
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

## Ranking

The `ranking_query` is used to rank text chunks based on the semantic similarity of the text chunk content and the input query.
This semantic similarity calculation is done via semantic embedding of the the `ranking_query` and each candidate text chunk into semantic vectors. A vector similarity search is done to find the text chunks with the most relevant semantic content to the query.


## Examples

### Keyword match

If a user is trying to search for information that matches a specific phrase or exact spelling, the `ranking_query` parameter alone will not help because it ranks ChunkDocuments based on a representation of the semantic meaning of the text chunk, but does not require that a specific phrase or spelling is present. In these cases, use a "String Operator" filter to require that all results match the exact keywords or phrases the user is interested in.

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


### `ranking_query` only searches the "text" attribute

The `ranking_query` only ranks semantic matches in the "text" attribute (which represents a chunk of a book's full text). Do not include content in the the `ranking_query` that is intended to match other attributes, like publish date or author's name. The `filters` can match based on other ChunkDocument attributes, so use the `filters` to search for content in those attributes. 

**Example:** The user wants to know about "Uruguayan literature from the 20th century” 

The `ranking_query` should not include "from the 20th century" because the period when a text is written is metadata, not direct text content. Instead you can use a filter on the appropriate non-text attribute to build a search that matches the user's intent. 

```json
{
  "ranking_query": "Uruguayan literature authors literary movements writing culture",
  "filters": ["publication_date", "Gte", "1900-01-01"]
}
```

Note how the temporal constraint "20th century" is applied via a publication_date filter rather than included in the semantic `ranking_query`.

### Un-searchable content

If the user asks to search for content that isn't available in any of the indexed attributes, do not include the information in the constructed search arguments because the request is unsearchable in the current data model. Let the user know what part of their query is unsearchable, and continue to execute a search using whatever is searchable for the user's query

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





Args:
  ranking_query: The query string used to rank text chunks in the search results. See the "Ranking" section of the tool description for more details.
   

  filters: (optional) The filter to apply to the query. See "Filtering" section of the tool description for syntax details.

  filters_match_null: (optional, defaults to True) When True, automatically modifies filters on potentially incomplete attributes (subject, language, publication_date, author) to also match ChunkDocuments where those attributes are null. This ensures search results include books with incomplete metadata. For example, a filter like `["subject", "ContainsAnyToken", "poetry"]` is automatically transformed to `["Or", [["subject", "ContainsAnyToken", "poetry"], ["subject", "Eq", null]]]`. Set to False if you specifically want to exclude results with missing metadata for filtered attributes.

<!-- 
    Args:
        ranking_query: The query string used to rank text chunks in the search results based on semantic similarity.
        filters: Optional filter specification to apply to the query. See tool documentation for syntax.
        filters_match_null: Optional boolean (default True) to include null-matching for incomplete attributes.
 -->