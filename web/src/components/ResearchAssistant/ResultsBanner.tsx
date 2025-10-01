import { Banner, Text } from "@nypl/design-system-react-components";
import React from "react";
import Link from "../Link/Link";

const ThumbsDownIcon: React.FC = () => {
    const bannerContent = (
        <Text>
            This tool only searches <strong>public domain scholarly e-books from our
            collections.</strong> To find other types of research content, search the{" "}
            <Link to="https://www.nypl.org/research/research-catalog/">
                Research Catalog
            </Link>{" "}
            (for physical research books) or{" "}
            <Link to="https://research.ebsco.com/c/tvrejk/search">Articles Plus</Link>{" "}
            (for digital research journals, articles, and databases).
        </Text>
    );
    return <Banner content={bannerContent} variant="warning" marginRight="l" marginY="s" />;
};

export default ThumbsDownIcon;
