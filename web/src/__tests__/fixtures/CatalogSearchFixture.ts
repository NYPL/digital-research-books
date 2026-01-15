import { Agent, FacetItem, WorkEdition } from "~/src/types/DataModel";
import { CatalogSearchResults } from "~/src/types/ResearchAssistant";

const mockFacets: { formats: FacetItem[]; languages: FacetItem[] } = {
  formats: [{ value: "ebook", count: 2 }],
  languages: [{ value: "eng", count: 2 }],
};

const mockPaging = {
  currentPage: 1,
  firstPage: 1,
  lastPage: 1,
  nextPage: 1,
  previousPage: 1,
  recordsPerPage: 10,
};

const createAgent = (name: string): Agent => ({
  name,
  roles: ["author"],
});

const createMockEdition = (overrides?: Partial<WorkEdition>): WorkEdition => ({
  edition_id: 1,
  title: "Default Edition",
  items: [],
  languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
  publication_date: "2023",
  ...overrides,
});

export const catalogResults: CatalogSearchResults = {
  facets: mockFacets,
  paging: mockPaging,
  totalWorks: 2,
  works: [
    {
      uuid: "1",
      title: "Test Book 1",
      sub_title: "First Edition",
      authors: [createAgent("Author One"), createAgent("Author Two")],
      dates: [{ date: "2023", type: "publication_date" }],
      editions: [
        createMockEdition({
          edition_id: 1,
          title: "Test Book 1",
          publication_date: "2023",
        }),
      ],
    },
    {
      uuid: "2",
      title: "Test Book 2",
      authors: [createAgent("Author Three")],
      dates: [{ date: "2022", type: "publication_date" }],
      editions: [
        createMockEdition({
          edition_id: 2,
          title: "Test Book 2",
          publication_date: "2022",
        }),
      ],
    },
  ],
  searchParams: {
    query: [["keyword", "test"]],
    filters: [],
  },
};

export const minimalCatalogResults: CatalogSearchResults = {
  facets: mockFacets,
  paging: mockPaging,
  totalWorks: 1,
  works: [
    {
      uuid: "3",
      title: "Minimal Book",
      authors: [],
      editions: [
        createMockEdition({
          edition_id: 3,
          title: "Minimal Book",
        }),
      ],
    },
  ],
  searchParams: {
    query: [["keyword", "minimal"]],
  },
};

export const multiPageCatalogResults: CatalogSearchResults = {
  ...catalogResults,
  totalWorks: 25,
  paging: {
    ...catalogResults.paging,
    lastPage: 3,
    nextPage: 2,
  },
};

export const emptyCatalogResults: CatalogSearchResults = {
  facets: { formats: [], languages: [] },
  paging: {
    currentPage: 1,
    firstPage: 1,
    lastPage: 1,
    nextPage: 1,
    previousPage: 1,
    recordsPerPage: 10,
  },
  totalWorks: 0,
  works: [],
  searchParams: {
    query: [["keyword", "nothing"]],
  },
};

export const singleAuthorCatalogResults: CatalogSearchResults = {
  facets: mockFacets,
  paging: mockPaging,
  totalWorks: 1,
  works: [
    {
      uuid: "4",
      title: "Single Author Book",
      authors: [createAgent("Solo Author")],
      editions: [
        createMockEdition({
          edition_id: 4,
          title: "Single Author Book",
        }),
      ],
    },
  ],
  searchParams: {
    query: [["keyword", "single"]],
  },
};

export const manyAuthorsCatalogResults: CatalogSearchResults = {
  facets: mockFacets,
  paging: mockPaging,
  totalWorks: 1,
  works: [
    {
      uuid: "5",
      title: "Many Authors Book",
      authors: Array.from({ length: 4 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
      editions: [
        createMockEdition({
          edition_id: 5,
          title: "Many Authors Book",
        }),
      ],
    },
  ],
  searchParams: {
    query: [["keyword", "many"]],
  },
};

export const longFieldsCatalogResults: CatalogSearchResults = {
  facets: mockFacets,
  paging: mockPaging,
  totalWorks: 1,
  works: [
    {
      uuid: "6",
      title: "A Very Long Title ".repeat(50),
      sub_title: "Edition with very long text ".repeat(10),
      authors: Array.from({ length: 20 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
      dates: [{ date: "2024", type: "publication_date" }],
      editions: [
        createMockEdition({
          edition_id: 6,
          title: "A Very Long Title ".repeat(50),
          publication_date: "2024",
        }),
      ],
    },
  ],
  searchParams: {
    query: [["keyword", "long"]],
  },
};
