import { Banner, Text } from "@nypl/design-system-react-components";
import React from "react";
import { RESEARCH_CATALOG_LINK } from "~/src/constants/links";
import Link from "../Link/Link";

const ResultsBanner: React.FC = () => {
  const bannerContent = (
    <Text>
      This tool only searches the <strong>Digitized Research Books</strong>{" "}
      collection. To search our other research collections, see the{" "}
      <Link to={RESEARCH_CATALOG_LINK} hasVisitedState={false}>
        Research homepage
      </Link>
      .
    </Text>
  );
  return (
    <Banner
      content={bannerContent}
      variant="warning"
      marginRight="l"
      sx={{
        a: { color: "ui.link.primary" },
      }}
    />
  );
};

export default ResultsBanner;
