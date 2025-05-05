import React from "react";
import { Icon } from "@nypl/design-system-react-components";
import CollectionUtils from "~/src/util/CollectionUtils";
import Link from "~/src/components/Link/Link";
import { OpdsLink } from "~/src/types/OpdsModel";
import { formatUrl } from "~/src/util/Util";
import { trackCtaClick } from "~/src/lib/adobe/Analytics";
import { trackEvent } from "~/src/lib/gtag/Analytics";

const DownloadLink: React.FC<{ author: string; links: OpdsLink[]; title: string }> = ({
  author,
  links,
  title,
}) => {
  const selectedLink = CollectionUtils.getDownloadLink(links);

  if (!selectedLink) return null;

  const formattedUrl = formatUrl(selectedLink.href);

  const trackDownloadCta = () => {
    trackCtaClick({
      cta_section: `${title}`,
      cta_text: "Download",
      destination_url: `${formattedUrl}`,
    });
    trackEvent({
      "event":  "file_download",
      "click_text": "Download PDF",
      "file_extension": selectedLink.type == "application/pdf" ? "pdf" : "epub",
      "file_name": title,
      "item_title": title,
      "item_author": author,
    });
  };

  if (selectedLink && selectedLink.href) {
    return (
      <Link
        to={`${formattedUrl}`}
        linkType="buttonSecondary"
        onClick={trackDownloadCta}
        aria-label={`${title} Download PDF`}
      >
        <Icon
          name="download"
          align="left"
          size="small"
          decorative
          iconRotation="rotate0"
        />
        Download PDF
      </Link>
    );
  }
};

export default DownloadLink;
