We refer to each text chunk in the search index as a "ChunkDocument".

## ChunkDocument Attributes

  text: 
    Data type: string
    Description: The full text of the text chunk.

  subject:
    Data type: array of string
    Description: A list of subjects that are associated with the book the text chunk belongs to. Subjects  are a mix of topics, genres, publication types, etc. Here is an example selection of subjects: 
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
    Data type: iso 8601 formatted date string
    Description: The publication date of the book the text chunk belongs to.

  author
    Data Type: array of strings.
    Description: An array of all the authors who contributed to the book the text chunk belongs to.

  title
    Data Type: string
    Description: The title of the book the text chunk belongs to.  


## Filtering

<!-- Changes to TP docs: changed "token" to "words" -->

Exact filters to apply to ChunkDocument attributes to refine search results. Think of it as a SQL WHERE clause.

Filters allow you to narrow down results by applying exact conditions to the returned ChunkDocument attributes. Conditions are arrays with an attribute name, operation, and value, for example:

  `["title", "Eq", "The Great Gatsby"]`
  `["author", "ContainsAnyToken", "Shakespeare Milton"]`
  `["publication_date", "NotEq", null]`

Values must have the same data type as the ChunkDocument attribute the filter is applied to, or an array of that type for operators like `ContainsAny`.

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

`Eq` (value) - Exact match for id or attributes values. If value is null, matches ChunkDocuments missing the attribute.

`NotEq` (value) - Inverse of Eq, for attributes values. If value is "null", matches ChunkDocuments with the attribute.

<!-- Removing to reduce complexity bc this can be handled with `Or` filter combination. "author" is the only field were I think this might be used -->
<!-- `In` (array[value]) - Matches any attributes values contained in the provided list.

`NotIn` (array[value]) - Inverse of In, matches any attributes values not contained in the provided list. -->

<!-- #### Array Attribute Operators

`Contains` (value) - Checks whether the selected array attribute contains the provided value.

`NotContains` (value) - Inverse of Contains.

`ContainsAny` (array[value]) - Checks whether the selected array attribute contains any of the values provided (intersection filter).

`NotContainsAny` (array[value]) - Inverse of ContainsAny. -->

#### Comparison Operators

`Lt` (value) - For ints, this is a numeric less-than on attributes values. For strings, lexicographic less-than. For datetimes, numeric less-than on millisecond representation.

`Lte` (value) - For ints, this is a numeric less-than-or-equal on attributes values. For strings, lexicographic less-than-or-equal. For datetimes, numeric less-than-or-equal on millisecond representation.

`Gt` (value) - For ints, this is a numeric greater-than on attributes values. For strings, lexicographic greater-than. For datetimes, numeric greater-than on millisecond representation.

`Gte` (value) - For ints, this is a numeric greater-than-or-equal on attributes values. For strings, lexicographic greater-than-or-equal. For datetimes, numeric greater-than-or-equal on millisecond representation.

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

#### String Operators
- String operators can only be used on string and array of strings typed attributes.  
- All string search operators are case insensitive.  
- "Tokens" in the string operator names means words; string operators break the filter string into a sequence of word tokens.  

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

If the user asks to search for some content that isn't available in any of the indexed attributes, do not include it in the search because it is unsearchable in the current data model. For example, if the user asks for book's that were first published as paperback's, you must not use this information to construct a search because the index does not include binding information about the book.


### Avoid ambiguous semantic queries

Avoid Homonyms or ambiguous meaning in `ranking_query`. 

Ambiguous example (bad): "I'm looking for a history of the several stories that exist under Bryant Park for the library" 
- This is bad because stories could mean tales or floors of a building. This ranking_query would mix text chunks that match both meanings high in the rankings and do a bad job at isolating a specific meaning.

Clear example (good): "history of the many floors of underground rooms that exist under Bryant Park for the library" 
- Clearly indicates one meaning.

If the user's actual search intent is ambiguous, ask for clarification rather than create an ambiguous `ranking_query`.



Args:
  ranking_query: The query string used to rank text chunks in the search results. See the "Ranking" section of the tool description for more details.
   

  filters: (optional) The filter to apply to the query. See "Filtering" section of the tool description for syntax details.

<!-- 
    Args:
        ranking_query: The query string used to rank text chunks in the search results based on semantic similarity.
        filters: Optional filter specification to apply to the query. See tool documentation for syntax.
 -->