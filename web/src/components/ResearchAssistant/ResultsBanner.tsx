import { Banner, Text } from "@nypl/design-system-react-components";
import React from "react";
import Link from "../Link/Link";
import { ARTICLES_PLUS_LINK, RESEARCH_CATALOG_LINK } from "~/src/constants/links";

const ResultsBanner: React.FC = () => {
    const bannerContent = (
        <Text>
            This tool only searches{" "}
            <strong>public domain scholarly e-books from our collections.</strong> To
            find other types of research content, search the{" "}
            <Link to={RESEARCH_CATALOG_LINK} hasVisitedState={false}>
                Research Catalog
            </Link>{" "}
            (for physical research books) or{" "}
            <Link to={ARTICLES_PLUS_LINK} hasVisitedState={false}>Articles Plus</Link>{" "}
            (for digital research journals, articles, and databases).
        </Text>
    );
    return (
        <Banner
            content={bannerContent}
            variant="warning"
            marginRight="l"
            marginTop="s"
            sx={{
                a: {color: "ui.link.primary"}
            }}
        />
    );
};

export default ResultsBanner;
