import { formatUrl, truncateStringOnWhitespace } from "~/src/util/Util";
import {
  MAX_PLACE_LENGTH,
  MAX_PUBLISHER_NAME_LENGTH,
  PLACEHOLDER_COVER_LINK,
} from "~/src/constants/editioncard";
import { MediaTypes } from "~/src/constants/mediaTypes";
import { Opds2Feed, OpdsLink } from "~/src/types/OpdsModel";

type ReadOnlineTypes = "readable" | "embedable";

export default class CollectionUtils {
  /** Get Cover Image
   * @param collection - The collection
   * @returns The URL of the cover that should be displayed.
   */
  static getCover(collection: Opds2Feed): string {
    if (!collection.publications || collection.publications.length === 0)
      return PLACEHOLDER_COVER_LINK;
    const coverLink = collection.publications[0].images.find((link) => {
      return MediaTypes.display.includes(link.type);
    });
    return coverLink ? formatUrl(coverLink.href) : PLACEHOLDER_COVER_LINK;
  }

  static getItemCover(collectionItem: OpdsPublication): string {
    const coverLink = collectionItem.images.find((link) => {
      return MediaTypes.display.includes(link.type);
    });
    return coverLink ? formatUrl(coverLink.href) : PLACEHOLDER_COVER_LINK;
  }

  // TODO: replace with collection_id property that will be added on backend response
  static getId(links: OpdsLink[]): string {
    if (!links || links.length === 0) return "";
    const link = links[0].href;
    const id = link.substring(link.lastIndexOf("/") + 1, link.indexOf("?"));
    return id[0] ?? "";
  }

  static getReadLink(
    links: OpdsLink[],
    type: ReadOnlineTypes
  ): undefined | OpdsLink {
    return links && links.find((link: OpdsLink) => link.identifier === type);
  }

  static getDownloadLink(links: OpdsLink[]): undefined | OpdsLink {
    return (
      links &&
      links.find((link: OpdsLink) => link.identifier === "downloadable")
    );
  }

  static getPublisherDisplayLocation(pubPlace: string): undefined | string {
    return (
      pubPlace &&
      ` in ${truncateStringOnWhitespace(pubPlace, MAX_PLACE_LENGTH)}`
    );
  }

  static getPublisherDisplayText(publisher: string): undefined | string {
    return (
      publisher &&
      ` by ${truncateStringOnWhitespace(publisher, MAX_PUBLISHER_NAME_LENGTH)}`
    );
  }

  static getEddLink(links: OpdsLink[]): undefined | OpdsLink {
    return (
      links &&
      links.find(
        (link) =>
          link.identifier === "requestable" || link.identifier === "catalog"
      )
    );
  }

  static getEditionLink(links: OpdsLink[]): undefined | OpdsLink {
    return links && links.find((link) => link.rel === "alternate");
  }
}
