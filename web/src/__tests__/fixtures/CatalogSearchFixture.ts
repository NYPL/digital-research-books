import { Agent } from "~/src/types/DataModel";
import {
  CatalogEdition,
  CatalogSearchResults,
  ConversationType,
  SearchParams,
} from "~/src/types/ResearchAssistant";

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

export const catalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [
    {
      id: 1,
      title: "Test Book 1",
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publication_date: "2023",
      publication_place: undefined,
      publishers: [],
      snippets: [],
      work_authors: [createAgent("Author One"), createAgent("Author Two")],
      work_title: "Test Book 1",
      work_uuid: "1",
    } as CatalogEdition,
    {
      id: 2,
      title: "Test Book 2",
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publication_date: "2022",
      publication_place: undefined,
      publishers: [],
      snippets: [],
      work_authors: [createAgent("Author Three")],
      work_title: "Test Book 2",
      work_uuid: "2",
    } as CatalogEdition,
  ],
  search_params: {
    query: [["keyword", "test"]],
    filters: [],
  } as SearchParams,
  paging: {
    recordsPerPage: mockPaging.recordsPerPage,
    firstPage: mockPaging.firstPage,
    previousPage: mockPaging.previousPage,
    currentPage: mockPaging.currentPage,
    nextPage: mockPaging.nextPage,
    lastPage: mockPaging.lastPage,
    totalRecords: 2,
  },
};

export const minimalCatalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [
    {
      id: 3,
      title: "Minimal Book",
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publishers: [],
      snippets: [],
      work_authors: [],
      work_title: "Minimal Book",
      work_uuid: "3",
    } as CatalogEdition,
  ],
  search_params: { query: [["keyword", "minimal"]] } as SearchParams,
  paging: {
    recordsPerPage: mockPaging.recordsPerPage,
    firstPage: mockPaging.firstPage,
    previousPage: mockPaging.previousPage,
    currentPage: mockPaging.currentPage,
    nextPage: mockPaging.nextPage,
    lastPage: mockPaging.lastPage,
    totalRecords: 1,
  },
};

export const multiPageCatalogResults: CatalogSearchResults = {
  ...catalogResults,
  paging: {
    ...catalogResults.paging,
    lastPage: 3,
    nextPage: 2,
    totalRecords: 25,
  },
};

export const emptyCatalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [],
  search_params: { query: [["keyword", "nothing"]] } as SearchParams,
  paging: {
    recordsPerPage: 10,
    firstPage: 1,
    previousPage: 1,
    currentPage: 1,
    nextPage: 1,
    lastPage: 1,
    totalRecords: 0,
  },
};

export const singleAuthorCatalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [
    {
      id: 4,
      title: "Single Author Book",
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publishers: [],
      snippets: [],
      work_authors: [createAgent("Solo Author")],
      work_title: "Single Author Book",
      work_uuid: "4",
    } as CatalogEdition,
  ],
  search_params: { query: [["keyword", "single"]] } as SearchParams,
  paging: {
    recordsPerPage: mockPaging.recordsPerPage,
    firstPage: mockPaging.firstPage,
    previousPage: mockPaging.previousPage,
    currentPage: mockPaging.currentPage,
    nextPage: mockPaging.nextPage,
    lastPage: mockPaging.lastPage,
    totalRecords: 1,
  },
};

export const manyAuthorsCatalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [
    {
      id: 5,
      title: "Many Authors Book",
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publishers: [],
      snippets: [],
      work_authors: Array.from({ length: 4 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
      work_title: "Many Authors Book",
      work_uuid: "5",
    } as CatalogEdition,
  ],
  search_params: { query: [["keyword", "many"]] } as SearchParams,
  paging: {
    recordsPerPage: mockPaging.recordsPerPage,
    firstPage: mockPaging.firstPage,
    previousPage: mockPaging.previousPage,
    currentPage: mockPaging.currentPage,
    nextPage: mockPaging.nextPage,
    lastPage: mockPaging.lastPage,
    totalRecords: 1,
  },
};

export const longFieldsCatalogResults: CatalogSearchResults = {
  conversation_context: ConversationType.Catalog,
  editions: [
    {
      id: 6,
      title: "A Very Long Title ".repeat(50),
      items: [],
      languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
      links: [],
      measurements: [],
      publication_date: "2024",
      publishers: [],
      snippets: [],
      work_authors: Array.from({ length: 20 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
      work_title: "A Very Long Title ".repeat(50),
      work_uuid: "6",
    } as CatalogEdition,
  ],
  search_params: { query: [["keyword", "long"]] } as SearchParams,
  paging: {
    recordsPerPage: mockPaging.recordsPerPage,
    firstPage: mockPaging.firstPage,
    previousPage: mockPaging.previousPage,
    currentPage: mockPaging.currentPage,
    nextPage: mockPaging.nextPage,
    lastPage: mockPaging.lastPage,
    totalRecords: 1,
  },
};
