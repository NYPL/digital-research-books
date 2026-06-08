import React from "react";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { Agent, ApiItem } from "~/src/types/DataModel";
import { CatalogItem } from "~/src/types/ResearchAssistant";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import DownloadLink from "./DownloadLink";
import EddLink from "./EddLink";
import OcrReadLink from "./OcrReadLink";
import ReadOnlineLink from "./ReadOnlineLink";

interface CtasProps {
  authors: Agent[];
  item: ApiItem | CatalogItem | undefined;
  title: string;
  workId?: string;
  editionId?: number;
}

const Ctas: React.FC<CtasProps> = ({
  authors,
  item,
  title,
  workId,
  editionId,
}) => {
  // cookies defaults to be undefined if not found
  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const loginCookie = cookies[NYPL_SESSION_ID];
  const isLoggedIn = !!loginCookie;

  const normalizedItem = item
    ? {
        ...item,
        links: item.links?.map((link) => ({
          ...link,
          mediaType: link.mediaType || link.media_type, // Normalize mediaType
        })),
      }
    : undefined;

  const readOnlineLink = EditionCardUtils.getReadOnlineLink(normalizedItem);
  const downloadLink = EditionCardUtils.selectDownloadLink(normalizedItem);

  const authorNames = authors ? authors.map((author) => author.name) : [];

  if (readOnlineLink || downloadLink) {
    return (
      <>
        {readOnlineLink &&
          (readOnlineLink.mediaType !== "application/ocr" ? (
            <ReadOnlineLink
              authors={authorNames}
              isLoggedIn={isLoggedIn}
              readOnlineLink={readOnlineLink}
              title={title}
            />
          ) : (
            <OcrReadLink
              readLink={readOnlineLink}
              workId={workId}
              editionId={editionId}
              title={title}
            />
          ))}
        {downloadLink && (
          <DownloadLink
            authors={authorNames}
            downloadLink={downloadLink}
            title={title}
            isLoggedIn={isLoggedIn}
            loginCookie={loginCookie}
          />
        )}
      </>
    );
  }

  const eddLink =
    normalizedItem && normalizedItem.links
      ? normalizedItem.links.find((link) => link.flags.edd)
      : undefined;

  // Offer EDD if available
  if (eddLink !== undefined) {
    return <EddLink eddLink={eddLink} isLoggedIn={isLoggedIn} title={title} />;
  }

  return <>Not yet available</>;
};

export default Ctas;
