import React from "react";
import {
  Card,
  CardActions,
  CardContent,
  CardHeading,
} from "@nypl/design-system-react-components";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { OpdsPublication } from "~/src/types/OpdsModel";
import CollectionUtils from "~/src/util/CollectionUtils";
import Ctas from "~/src/components/CollectionItemCard/Ctas";
import EditionYear from "~/src/components/CollectionItemCard/EditionYear";
import PublisherAndLocation from "~/src/components/CollectionItemCard/PublisherAndLocation";
import CopyrightLink from "~/src/components/CollectionItemCard/CopyrightLink";
import LanguageDisplayText from "~/src/components/CollectionItemCard/LanguageDisplayText";

// Creates an Collection item card out of the collectionItem object
export const CollectionItemCard: React.FC<{
  author: string | undefined;
  collectionItem: OpdsPublication;
}> = ({ author, collectionItem }) => {
  // cookies defaults to be undefined if not found
  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const { links, metadata } = collectionItem;
  const { locationCreated, published, rights, language, title, publisher } =
    metadata;

  return (
    <Card
      imageProps={{
        src: CollectionUtils.getItemCover(collectionItem),
        size: "xsmall",
        aspectRatio: "original",
        alt: ``,
      }}
      layout="row"
      isBordered
      isAlignedRightActions
      p="s"
    >
      <CardHeading
        level="h5"
        size="heading6"
        sx={{
          fontSize: "18px",
          a: {
            textDecoration: "none",
          },
        }}
      >
        <EditionYear links={links} published={published} />
      </CardHeading>
      <CardContent>
        <PublisherAndLocation
          pubPlace={locationCreated}
          publisher={publisher}
        />
        <LanguageDisplayText language={language} />
        <CopyrightLink rights={rights} />
      </CardContent>
      <CardActions display="flex" flexDir="column" whiteSpace="nowrap" gap={4}>
        <Ctas
          author={author}
          links={links}
          title={title}
          isLoggedIn={!!cookies[NYPL_SESSION_ID]}
        />
      </CardActions>
    </Card>
  );
};
