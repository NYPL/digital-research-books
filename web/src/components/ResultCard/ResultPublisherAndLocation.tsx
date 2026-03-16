import { Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import {
  MAX_PLACE_LENGTH,
  MAX_PUBLISHER_NAME_LENGTH,
} from "~/src/constants/editioncard";
import { RESULT_PUBLISHER_TEST_ID } from "~/src/constants/testIds";
import { Agent } from "~/src/types/DataModel";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import EllipseSpacer from "./EllipseSpacer";

const ResultPublisherAndLocation: React.FC<{
  pubPlace: string;
  publishers: Agent[];
}> = ({ pubPlace, publishers }) => {
  const displayLocation = pubPlace
    ? truncateStringOnWhitespace(pubPlace, MAX_PLACE_LENGTH).trim()
    : "";

  const publisherNames: string[] = (publishers || [])
    .map((p) => p && p.name)
    .filter(Boolean) as string[];

  let displayName = "";
  if (publisherNames.length === 1) {
    displayName = truncateStringOnWhitespace(
      publisherNames[0],
      MAX_PUBLISHER_NAME_LENGTH
    );
  } else if (publisherNames.length > 1) {
    displayName = `${truncateStringOnWhitespace(
      publisherNames[0],
      MAX_PUBLISHER_NAME_LENGTH
    )} + ${publisherNames.length - 1} more`;
  }

  if (!displayName && !displayLocation) return <></>;

  return (
    <Flex flexDir="row" alignItems="center" data-testid={RESULT_PUBLISHER_TEST_ID}>
      {displayName && (
        <Text as="span" whiteSpace="normal">
          {displayName}
        </Text>
      )}
      {displayName && displayLocation && <EllipseSpacer />}
      {displayLocation && (
        <Text as="span" whiteSpace="normal">
          {displayLocation}
        </Text>
      )}
      {(displayName || displayLocation) && <EllipseSpacer />}
    </Flex>
  );
};

export default ResultPublisherAndLocation;
