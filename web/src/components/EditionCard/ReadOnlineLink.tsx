import { Box } from "@nypl/design-system-react-components";
import React from "react";
import Link from "~/src/components/Link/Link";
import { LOGIN_LINK_BASE } from "~/src/constants/links";
import { ItemLink } from "~/src/types/DataModel";
import { LOGIN_TO_READ_TEST_ID } from "~/src/constants/testIds";
import { getHostname } from "~/src/util/LinkUtils";
import { trackEvent } from "~/src/lib/gtag/Analytics";

// "Read Online" button should only show up if the link was flagged as "reader" or "embed"
const ReadOnlineLink: React.FC<{
  authors: string[];
  isLoggedIn: boolean;
  readOnlineLink: ItemLink;
  title: string;
  loginCookie?: any;
}> = ({  authors, isLoggedIn, readOnlineLink, title }) => {
  let linkText = "Read Online";
  let linkUrl: any = {
    pathname: `/read/${readOnlineLink.link_id}`,
  };

  if (
    (readOnlineLink.flags.nypl_login ||
      readOnlineLink.flags.fulfill_limited_access) &&
    !isLoggedIn
  ) {
    linkText = "Log in to read online";
    linkUrl = LOGIN_LINK_BASE + encodeURIComponent(window.location.href);
  }

  const trackReadOnlineClick = () => {
    if (linkUrl.pathname) {
      const hostname = getHostname();
      trackEvent({
        "event":  "digital_read_online",
        "item_title": title,
        "item_author": authors,
        "read_online_url": `${hostname}${linkUrl.pathname}`
      });
    }
  }

  return (
    readOnlineLink && (
      <Box data-testid={LOGIN_TO_READ_TEST_ID}>
        <Link
          to={linkUrl}
          linkType="button"
          aria-label={`${title} ${linkText}`}
          onClick={trackReadOnlineClick}
        >
          {linkText}
        </Link>
      </Box>
    )
  );
};

export default ReadOnlineLink;
