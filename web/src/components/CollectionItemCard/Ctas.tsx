import React from "react";
import { OpdsLink } from "~/src/types/OpdsModel";
import ReadOnlineLink from "~/src/components/CollectionItemCard/ReadOnlineLink";
import DownloadLink from "~/src/components/CollectionItemCard/DownloadLink";
import EddLink from "~/src/components/CollectionItemCard/EddLink";
import CollectionUtils from "~/src/util/CollectionUtils";

const Ctas: React.FC<{
  author: string;
  links: OpdsLink[];
  title: string;
  isLoggedIn: boolean;
}> = ({ author, links, title, isLoggedIn }) => {
  const eddLink = CollectionUtils.getEddLink(links);

  if (links) {
    return (
      <>
        {/* If a digital version exists, link directly */}
        <ReadOnlineLink author={author} links={links} title={title} />
        <DownloadLink author={author} links={links} title={title} />
      </>
    );
  }

  if (eddLink) {
    return <EddLink eddLink={eddLink} isLoggedIn={isLoggedIn} title={title} />;
  }

  return <>Not yet available</>;
};

export default Ctas;
