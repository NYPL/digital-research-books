import React from "react";
import Link from "~/src/components/Link/Link";
import { LOGIN_LINK_BASE } from "~/src/constants/links";
import { ItemLink } from "~/src/types/DataModel";

const EddLink: React.FC<{
  eddLink: ItemLink;
  isLoggedIn: boolean;
  title: string;
}> = ({ eddLink, isLoggedIn, title }) => {
  const currentUrl = typeof window !== "undefined" ? window.location.href : "";

  if (isLoggedIn) {
    return (
      <>
        <Link
          // Url starts with www
          to={`https://${eddLink.url}`}
          variant="buttonPrimary"
          target="_blank"
          aria-label={`Request scan for ${title}`}
          bgColor="section.research.secondary"
          width="fit-content"
        >
          Request scan
        </Link>
      </>
    );
  } else {
    return (
      <>
        <Link
          to={`${LOGIN_LINK_BASE}${encodeURIComponent(currentUrl)}`}
          variant="buttonPrimary"
          aria-label={`Log in to request scan for ${title}`}
          bgColor="section.research.secondary"
          width="fit-content"
        >
          Log in to request scan
        </Link>
      </>
    );
  }
};

export default EddLink;
