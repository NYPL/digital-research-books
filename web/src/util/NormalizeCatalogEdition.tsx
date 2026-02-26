import { ApiItem, ItemLink, Rights, WorkEdition } from "~/src/types/DataModel";
import {
  CatalogEdition,
  CatalogItem,
  CatalogLink,
  CatalogRight,
} from "~/src/types/ResearchAssistant";
import { ApiWork } from "~/src/types/WorkQuery";

function mapCatalogToWorkEdition(edition: CatalogEdition): WorkEdition {
  return {
    edition_id: edition.id,
    title: edition.title,
    publication_place: edition.publication_place,
    publication_date: edition.publication_date,
    publishers: edition.publishers,
    languages: edition.languages,
    links: edition.links,
    items: edition.items
      ? edition.items.map(mapCatalogItemToApiItem)
      : undefined,
    summary: edition.summary,
    work_id: edition.work_id,
    work_uuid: edition.work_uuid,
  };
}

export function normalizeCatalogEditionsToApiWorks(
  catalogEditions: CatalogEdition[]
): ApiWork[] {
  const works = new Map<string, ApiWork>();

  for (const edition of catalogEditions) {
    const uuid = edition.work_uuid ?? String(edition.work_id ?? edition.title);
    const work = works.get(uuid) ?? {
      uuid,
      title: edition.work_title ?? edition.title,
      authors: edition.work_authors ?? [],
      editions: [] as WorkEdition[],
      edition_count: 0,
      languages: edition.work_languages,
      subjects: edition.work_subjects,
      series: edition.work_series ?? undefined,
      dates: edition.work_dates ?? undefined,
    };

    const mappedEdition = mapCatalogToWorkEdition(edition);
    work.editions = work.editions
      ? [...work.editions, mappedEdition]
      : [mappedEdition];
    work.edition_count = work.editions?.length ?? 1;

    works.set(uuid, work);
  }

  return Array.from(works.values());
}

function mapCatalogLinkToItemLink(link: CatalogLink): ItemLink {
  return {
    link_id: link.id,
    mediaType: link.media_type,
    url: link.url,
    flags: link.flags,
  };
}

function mapCatalogRightToRight(right: CatalogRight): Rights {
  return {
    source: right.source,
    license: right.license,
    rightsStatement: right.rights_statement,
  };
}

function mapCatalogItemToApiItem(item: CatalogItem): ApiItem {
  return {
    content_type: item.content_type,
    contributors: item.contributors,
    drm: item.drm,
    item_id: item.id,
    links: item.links ? item.links.map(mapCatalogLinkToItemLink) : undefined,
    location: item.physical_location,
    measurements: item.measurements,
    rights: item.rights ? item.rights.map(mapCatalogRightToRight) : undefined,
    source: item.source,
  };
}
