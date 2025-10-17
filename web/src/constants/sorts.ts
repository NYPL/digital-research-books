export const sortMap = {
  Relevance: { field: "relevance", dir: "DESC" },
  "Title A-Z": { field: "title", dir: "ASC" },
  "Title Z-A": { field: "title", dir: "DESC" },
  "Author A-Z": { field: "author", dir: "ASC" },
  "Author Z-A": { field: "author", dir: "DESC" },
  "Year Published (Old-New)": { field: "date", dir: "ASC" },
  "Year Published (New-Old)": { field: "date", dir: "DESC" },
};

export const vraSortMap = {
  Relevance: { field: "relevance", dir: "DESC" },
  "Title (A - Z)": { field: "title", dir: "ASC" },
  "Title (Z - A)": { field: "title", dir: "DESC" },
  "Author (A - Z)": { field: "author", dir: "ASC" },
  "Author (Z - A)": { field: "author", dir: "DESC" },
  "Date (Old - New)": { field: "date", dir: "ASC" },
  "Date (New - Old)": { field: "date", dir: "DESC" },
};

export const collectionSortMap = {
  Relevance: { field: "relevance", dir: "DESC" },
  "Title A-Z": { field: "title", dir: "ASC" },
  "Creator A-Z": { field: "creator", dir: "ASC" },
  "Creation Date (Old-New)": { field: "created", dir: "ASC" },
};

export const numbersPerPage = ["10", "20", "50", "100"];

const sorts = { sortMap, collectionSortMap, numbersPerPage };

export default sorts;
