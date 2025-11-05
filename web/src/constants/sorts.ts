export const sortMap = {
  Relevance: { field: "relevance", dir: "DESC" },
  "Title A-Z": { field: "title", dir: "ASC" },
  "Title Z-A": { field: "title", dir: "DESC" },
  "Author A-Z": { field: "author", dir: "ASC" },
  "Author Z-A": { field: "author", dir: "DESC" },
  "Year Published (Old-New)": { field: "date", dir: "ASC" },
  "Year Published (New-Old)": { field: "date", dir: "DESC" },
};

export const sortOptions = [
  {
    id: "relevance",
    label: "Relevance",
    value: { field: "relevance", dir: "DESC" },
  },
  {
    id: "titleAsc",
    label: "Title (A - Z)",
    value: { field: "title", dir: "ASC" },
  },
  {
    id: "titleDes",
    label: "Title (Z - A)",
    value: { field: "title", dir: "DESC" },
  },
  {
    id: "authorAsc",
    label: "Author (A - Z)",
    value: { field: "author", dir: "ASC" },
  },
  {
    id: "authorDes",
    label: "Author (Z - A)",
    value: { field: "author", dir: "DESC" },
  },
  {
    id: "dateAsc",
    label: "Date (Old - New)",
    value: { field: "date", dir: "ASC" },
  },
  {
    id: "dateDes",
    label: "Date (New - Old)",
    value: { field: "date", dir: "DESC" },
  },
];

export const collectionSortMap = {
  Relevance: { field: "relevance", dir: "DESC" },
  "Title A-Z": { field: "title", dir: "ASC" },
  "Creator A-Z": { field: "creator", dir: "ASC" },
  "Creation Date (Old-New)": { field: "created", dir: "ASC" },
};

export const numbersPerPage = ["10", "20", "50", "100"];

const sorts = { sortMap, collectionSortMap, numbersPerPage };

export default sorts;
