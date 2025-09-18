import React from "react";
import Link from "~/src/components/Link/Link";
import { LOGIN_LINK_BASE } from "~/src/constants/links";
import { ItemLink } from "~/src/types/DataModel";

const EddLink: React.FC<{
  eddLink: ItemLink;
  isLoggedIn: boolean;
  title: string;
}> = ({ eddLink, isLoggedIn, title }) => {
  if (isLoggedIn) {
    return (
      <>
        <Link
          // Url starts with www
          to={`https://${eddLink.url}`}
          linkVariant="buttonPrimary"
          target="_blank"
          aria-label={`Request scan for ${title}`}
        >
          Request Scan
        </Link>
      </>
    );
  } else {
    return (
      <>
        <Link
          to={`${LOGIN_LINK_BASE}${encodeURIComponent(window.location.href)}`}
          linkVariant="buttonSecondary"
          aria-label={`Log in to request scan for ${title}`}
        >
          Log in to request scan
        </Link>
      </>
    );
  }
};

export default EddLink;
