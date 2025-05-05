import React from "react";
import { OpdsLink } from "~/src/types/OpdsModel";
import CollectionUtils from "~/src/util/CollectionUtils";
import { formatUrl } from "~/src/util/Util";
import Link from "~/src/components/Link/Link";
import { trackEvent } from "~/src/lib/gtag/Analytics";

// "Read Online" button should only show up if the link was flagged as "reader" or "embed"
const ReadOnlineLink: React.FC<{  author: string; links: OpdsLink[]; title: string; }> = ({
  author,
  links,
  title,
}) => {
  const localLink = CollectionUtils.getReadLink(links, "readable");
  const embeddedLink = CollectionUtils.getReadLink(links, "embedable");

  // Prefer local link over embedded link
  const readOnlineLink = localLink ?? embeddedLink;

  if (!readOnlineLink) return null;

  const trackReadOnlineClick = () => {
    trackEvent({
      "event":  "digital_read_online",
      "item_title": title,
      "item_author": author,
      "read_online_url": readOnlineLink.href
    });
  }

  return (
    <Link
      to={{
        pathname: formatUrl(readOnlineLink.href),
      }}
      linkType="button"
      aria-label={`${title} Read Online`}
      onClick={trackReadOnlineClick}
    >
      Read Online
    </Link>
  );
};

export default ReadOnlineLink;
