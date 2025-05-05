import { Box, Heading } from "@nypl/design-system-react-components";
import React from "react";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import { OpdsPublication } from "~/src/types/OpdsModel";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import Author from "../CollectionItemCard/Author";
import { CollectionItemCard } from "../CollectionItemCard/CollectionItemCard";
import Link from "../Link/Link";

export const CollectionItem: React.FC<{
  publication: OpdsPublication;
}> = ({ publication }) => {
  const { links, metadata } = publication;
  const { creator, subtitle, title } = metadata;

  return (
    <Box>
      <Heading
        level="h4"
        marginBottom="xs"
        sx={{
          a: {
            textDecoration: "none",
          },
        }}
      >
        <Link
          to={{
            pathname: links.find((link) => link.rel === "alternate").href,
          }}
        >
          {truncateStringOnWhitespace(title, MAX_TITLE_LENGTH)}
        </Link>
      </Heading>
      {subtitle && <Box marginBottom="xs">{subtitle}</Box>}
      {creator && (
        <Box marginBottom="xs">
          By <Author author={creator} />{" "}
        </Box>
      )}
      <CollectionItemCard author={creator} collectionItem={publication}></CollectionItemCard>
    </Box>
  );
};
