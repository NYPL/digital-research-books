import { Agent } from "~/src/types/DataModel";
import {
  CatalogEdition,
  CatalogItem,
  CatalogLink,
  CatalogSearchResults,
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

const defaultFlags = {
  catalog: false,
  download: false,
  embed: false,
  reader: false,
};

let nextLinkId = 1;

export const createLink = (
  overrides: Partial<CatalogLink> = {}
): CatalogLink => ({
  id: nextLinkId++,
  url: "https://example.org/resource",
  media_type: "text/html",
  content: null,
  flags: { ...defaultFlags },
  ...overrides,
});

export const createItem = (
  overrides?: Partial<CatalogItem> & { linkOverrides?: Partial<CatalogLink>[] }
): CatalogItem => {
  const { linkOverrides, ...rest } = overrides || {};

  return {
    id: 2001,
    edition_id: 1001,
    content_type: "ebook",
    contributors: [],
    drm: null,
    links: linkOverrides
      ? linkOverrides.map((lo) => createLink(lo))
      : [
          createLink({
            media_type: "text/html",
            flags: { ...defaultFlags, embed: true },
          }),
          createLink({
            media_type: "application/ocr",
            flags: { ...defaultFlags, reader: true },
          }),
        ],
    measurements: null,
    physical_location: null,
    rights: [
      {
        license: "public_domain",
        rights_statement: "Public Domain",
        source: "test-source",
      },
    ],
    source: "grin",
    ...rest,
  };
};

const defaultLanguage = {
  language: "English",
  iso_2: "en",
  iso_3: "eng",
};

const baseEdition: Partial<CatalogEdition> = {
  items: [],
  languages: [defaultLanguage],
  links: [],
  measurements: [],
  publishers: [],
  snippets: [],
};

export const createEdition = (
  overrides: Partial<CatalogEdition> & { snippetCount?: number } = {}
): CatalogEdition => {
  const { snippetCount = 1, ...rest } = overrides;

  const snippets =
    rest.snippets ??
    Array.from({ length: snippetCount }, (_, i) => ({
      chunk_score: 0.5 - i * 0.1,
      start_page: i + 1,
      end_page: i + 10,
      item_id: 2001,
      text: `Test snippet ${i + 1}`,
    }));

  return {
    ...baseEdition,
    id: 1001,
    title: "Test Edition Title",
    publication_date: "2024",
    publication_place: null,
    work_authors: [createAgent("Test Author")],
    work_title: "Test Title",
    work_uuid: "test-work-uuid-1",
    alt_titles: [],
    contributors: [],
    dates: [{ date: "2024", type: "publication_date" }],
    edition: null,
    edition_statement: "1st ed",
    extent: "100 p.",
    sub_title: null,
    summary: "Test summary",
    table_of_contents: null,
    volume: null,
    work: [],
    work_alt_titles: [],
    work_contributors: [],
    work_dates: null,
    work_id: 4001,
    work_languages: [],
    work_measurements: [],
    work_medium: null,
    work_series: null,
    work_series_position: null,
    work_sub_title: null,
    work_subjects: [],
    snippets,
    ...rest,
  } as CatalogEdition;
};

export const createWork = (edition: CatalogEdition, overrides?: any) => ({
  uuid: edition.work_uuid,
  title: edition.work_title,
  editions: [edition],
  edition_count: 1,
  ...overrides,
});

const createPaging = (
  totalRecords = 0,
  overrides: Partial<CatalogSearchResults["paging"]> = {}
): CatalogSearchResults["paging"] => ({
  ...mockPaging,
  totalRecords,
  ...overrides,
});

export const createCatalogResults = ({
  editions = [],
  query = "test",
  totalRecords = editions.length,
  paging = {},
}: {
  editions?: CatalogEdition[];
  query?: string;
  totalRecords?: number;
  paging?: Partial<CatalogSearchResults["paging"]>;
} = {}): CatalogSearchResults => ({
  editions,
  search_params: {
    query: [["keyword", query]],
    filters: [],
  },
  paging: createPaging(totalRecords, paging),
});

export const catalogResults = createCatalogResults({
  query: "test",
  totalRecords: 2,
  editions: [
    createEdition({
      id: 1,
      title: "Test Book 1",
      work_title: "Test Book 1",
      work_uuid: "1",
      publication_date: "2023",
      publication_place: undefined,
      work_authors: [createAgent("Author One"), createAgent("Author Two")],
    }),
    createEdition({
      id: 2,
      title: "Test Book 2",
      work_title: "Test Book 2",
      work_uuid: "2",
      publication_date: "2022",
      publication_place: undefined,
      work_authors: [createAgent("Author Three")],
    }),
  ],
});

export const minimalCatalogResults = createCatalogResults({
  query: "minimal",
  editions: [
    createEdition({
      id: 3,
      title: "Minimal Book",
      work_title: "Minimal Book",
      work_uuid: "3",
      work_authors: [],
      snippetCount: 0,
    }),
  ],
});

export const multiPageCatalogResults = createCatalogResults({
  query: "test",
  editions: catalogResults.editions,
  totalRecords: 25,
  paging: { lastPage: 3, nextPage: 2 },
});

export const emptyCatalogResults = createCatalogResults({
  query: "nothing",
});

export const singleAuthorCatalogResults = createCatalogResults({
  query: "single",
  editions: [
    createEdition({
      id: 4,
      title: "Single Author Book",
      work_title: "Single Author Book",
      work_uuid: "4",
      work_authors: [createAgent("Solo Author")],
    }),
  ],
});

export const manyAuthorsCatalogResults = createCatalogResults({
  query: "many",
  editions: [
    createEdition({
      id: 5,
      title: "Many Authors Book",
      work_title: "Many Authors Book",
      work_uuid: "5",
      work_authors: Array.from({ length: 4 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
    }),
  ],
});

export const longFieldsCatalogResults = createCatalogResults({
  query: "long",
  editions: [
    createEdition({
      id: 6,
      title: "A Very Long Title ".repeat(50),
      work_title: "A Very Long Title ".repeat(50),
      work_uuid: "6",
      publication_date: "2024",
      work_authors: Array.from({ length: 20 }, (_, i) =>
        createAgent(`Author ${i + 1}`)
      ),
    }),
  ],
});
