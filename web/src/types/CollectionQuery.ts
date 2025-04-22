import { Opds2Feed } from "./OpdsModel";

export type ApiCollectionQuery = {
  sort?: string;
  page?: number;
};

export type CollectionQuery = {
  identifier?: string;
  page?: number;
  perPage?: number;
  sort?: string;
};

export const CollectionQueryDefaults: CollectionQuery = {
  identifier: "",
  page: 1,
  perPage: 10,
  sort: "relevance",
};

export type CollectionResult = {
  collections?: Opds2Feed;
  statusCode?: number;
};
