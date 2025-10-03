import React from "react";
import { OpdsLink } from "~/src/types/OpdsModel";
import Link from "~/src/components/Link/Link";
import { LOGIN_LINK_BASE } from "~/src/constants/links";

export const EddLink: React.FC<{
  eddLink: OpdsLink;
  isLoggedIn: boolean;
  title: string;
}> = ({ eddLink, isLoggedIn, title }) => {
  if (isLoggedIn) {
    return (
      <>
        <Link
          // Url starts with www
          to={`https://${eddLink.href}`}
          variant="buttonPrimary"
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
          variant="buttonPrimary"
          aria-label={`Log in to request scan for ${title}`}
        >
          Log in to request scan
        </Link>
      </>
    );
  }
};

export default EddLink;
