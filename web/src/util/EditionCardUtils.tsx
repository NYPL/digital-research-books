import {
  Agent,
  Instance,
  ApiItem,
  ItemLink,
  Language,
  WorkEdition,
  Identifier,
} from "../types/DataModel";
import { formatUrl, truncateStringOnWhitespace } from "./Util";
import {
  MAX_PLACE_LENGTH,
  MAX_PUBLISHER_NAME_LENGTH,
  MAX_SUBTITILE_LENGTH,
  PLACEHOLDER_COVER_LINK,
} from "../constants/editioncard";
import { MediaTypes } from "../constants/mediaTypes";
import { NextRouter } from "next/router";
import { LOGIN_LINK_BASE } from "../constants/links";

// EditionCard holds all the methods needed to build an Edition Card
export default class EditionCardUtils {
  static getPreferredAgent(agents: any, role: any) {
    if (!agents || !agents.length) return undefined;

    const viafAgents = agents.filter((agent: any) => agent.viaf !== null);
    if (viafAgents && viafAgents.length) {
      const foundAuthors = viafAgents.filter((agent: any) =>
        agent.roles.includes(role)
      );
      if (foundAuthors && foundAuthors.length) {
        return foundAuthors;
      }
    }

    const preferredAgents = agents.find((agent: any) =>
      agent.roles.includes(role)
    );
    return preferredAgents ? [preferredAgents] : undefined;
  }

  static editionYearText(edition: WorkEdition) {
    return edition && edition.publication_date
      ? `${edition.publication_date} Edition`
      : "Edition Year Unknown";
  }

  static getFirstAndCountMore(array: any) {
    let moreText;
    if (array.length <= 1) {
      moreText = "";
    } else {
      moreText = ` + ${array.length - 1} more`;
    }

    return `${truncateStringOnWhitespace(
      array[0],
      MAX_PUBLISHER_NAME_LENGTH
    )}${moreText}`;
  }

  // Subtitle
  static getSubtitle(subtitle: string | undefined): string {
    if (!subtitle) {
      return undefined;
    }
    return truncateStringOnWhitespace(subtitle, MAX_SUBTITILE_LENGTH);
  }

  // Author
  static getAuthorIdentifier(author: Agent) {
    return (
      (author.viaf && ["viaf", "viaf"]) ||
      (author.lcnaf && ["lcnaf", "lcnaf"]) || ["name", "author"]
    );
  }

  /** Get Cover Image
   * @param covers - The list of covers
   * @returns The URL of the cover that should be displayed.
   */

  static getCover(links: ItemLink[]): string {
    if (!links || links.length === 0) return PLACEHOLDER_COVER_LINK;
    const coverLink = links.find((link) => {
      return MediaTypes.display.includes(link.mediaType);
    });
    return coverLink ? formatUrl(coverLink.url) : PLACEHOLDER_COVER_LINK;
  }

  static getPublisherDisplayLocation(pubPlace: string): undefined | string {
    return (
      pubPlace &&
      ` in ${truncateStringOnWhitespace(pubPlace, MAX_PLACE_LENGTH)}`
    );
  }

  static getPublishersDisplayText(publishers: Agent[]): undefined | string {
    if (!publishers || publishers.length === 0) return "";
    const publisherNames = publishers.map(
      (pubAgent: Agent) => pubAgent && pubAgent.name
    );
    return ` by ${EditionCardUtils.getFirstAndCountMore(publisherNames)}`;
  }

  static getUpPublisher(publishers: Agent[]): undefined | string {
    if (!publishers || publishers.length === 0) return "";
    const publisherNames = publishers.map(
      (pubAgent: Agent) => pubAgent && pubAgent.name
    );
    return EditionCardUtils.getFirstAndCountMore(publisherNames);
  }

  // Language Display
  static getLanguageDisplayText(previewEdition: WorkEdition): string {
    if (
      previewEdition &&
      previewEdition.languages &&
      previewEdition.languages.length
    ) {
      const languagesTextList = previewEdition.languages
        .filter((lang: Language) => {
          return lang && lang.language;
        })
        .map((lang: Language) => lang.language);
      if (languagesTextList && languagesTextList.length) {
        const languageText = `Languages: ${languagesTextList.join(", ")}`;
        return languageText;
      }
    }
    return "Languages: Undetermined";
  }

  // Rights
  static getLicense(item: ApiItem): string {
    return item && item.rights && item.rights.length > 0
      ? `Copyright: ${item.rights[0].rightsStatement}`
      : "Copyright: Unknown";
  }

  static getReadLink = (item: ApiItem, type: "reader" | "embed"): ItemLink => {
    if (!item || !item.links) return undefined;
    return item.links.find((link: ItemLink) => {
      return link.flags[type];
    });
  };

  static selectDownloadLink = (item: ApiItem): ItemLink => {
    if (!item || !item.links) return undefined;
    return item.links.find((link: ItemLink) => {
      return link.flags["download"];
    });
  };

  static getUpLink = (item: ApiItem): ItemLink => {
    if (!item || !item.links) return undefined;
    return item.links.find((link: ItemLink) => {
      return !link.flags.edd && link.flags.nypl_login;
    });
  };

  // "Read Online" button should only show up if the link was flagged as "reader" or "embed"
  static getReadOnlineLink = (item: ApiItem) => {
    const localLink = EditionCardUtils.getReadLink(item, "reader");
    const embeddedLink = EditionCardUtils.getReadLink(item, "embed");
    // Prefer local link over embedded link
    const readOnlineLink = localLink ?? embeddedLink;

    return readOnlineLink;
  };

  static getOclcLink(instance: Instance): string {
    const oclc =
      instance && instance.identifiers
        ? instance.identifiers.find(
            (identifier: Identifier) => identifier.authority === "oclc"
          )
        : undefined;
    const oclcLink = oclc
      ? `https://www.worldcat.org/oclc/${oclc.identifier}`
      : undefined;

    return oclcLink;
  }

  // return first item if links are available
  static getPreviewItem(items: ApiItem[] | undefined) {
    if (!items) return undefined;

    const firstItem = items[0];

    return firstItem && firstItem.links ? firstItem : undefined;
  }

  static isAvailableOnline(item: ApiItem) {
    return (
      item &&
      item.links &&
      item.links.find((link: ItemLink) => {
        return (
          link.flags["reader"] || link.flags["embed"] || link.flags["download"]
        );
      })
    );
  }

  static isPhysicalEdition(item: ApiItem): boolean {
    const availableOnline = this.isAvailableOnline(item);
    const eddLink =
      item && item.links
        ? item.links.find((link) => link.flags.edd)
        : undefined;

    return !availableOnline && eddLink !== undefined;
  }

  static isUniversityPress(item: ApiItem): boolean {
    const universityPress =
      item && item.links
        ? item.links.find((link) => !link.flags.edd && link.flags.nypl_login)
        : undefined;
    return universityPress !== undefined;
  }

  static isPublicDomain(item: ApiItem): boolean {
    const regex = /public[ _]?domain( \(x\))?/i
    const publicDomain = item && item.rights && item.rights.length > 0
      ? item.rights.find((right) => regex.test(right.rightsStatement))
      : undefined;
    return publicDomain !== undefined;
  }

  static createGetContent = (nyplIdentityCookie: any, router: NextRouter) => {
    const fetchWithAuth = async (fulfillUrl: string, proxyUrl?: string) => {
      const url = new URL(fulfillUrl);
      const res = await fetch(url.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${nyplIdentityCookie.access_token}`,
        },
      });

      if (res.ok) {
        // Generate the resource URL using the proxy
        const resourceUrl = res.url;
        const proxiedUrl: string = proxyUrl
          ? `${proxyUrl}${encodeURIComponent(resourceUrl)}`
          : resourceUrl;
        const response = await fetch(proxiedUrl, { mode: "cors" });
        const resourceAsByteArray = new Uint8Array(
          await response.arrayBuffer()
        );

        if (!response.ok) {
          throw new Error("Response not Ok for URL: " + url);
        }
        return resourceAsByteArray;
      } else {
        // redirect to the NYPL login page if access token is invalid
        if (res.status === 401) {
          router.push(
            LOGIN_LINK_BASE + encodeURIComponent(window.location.href)
          );
        }
      }
    };
    return fetchWithAuth;
  };
}
