import { Flex, Icon, Text } from "@nypl/design-system-react-components";
import React from "react";
import { Agent } from "~/src/types/DataModel";
import EditionCardUtils from "~/src/util/EditionCardUtils";

const UpBlurb: React.FC<{ publishers: Agent[] }> = ({ publishers }) => {
  const publisher = EditionCardUtils.getUpPublisher(publishers);
  return (
    <Flex
      alignItems={{ base: "start", md: "center" }}
      marginTop={{ base: "s", md: "m" }}
      marginBottom={{ base: "s", lg: "0" }}
    >
      <Icon name="errorOutline" size="small" />
      <Text size="caption" marginLeft="xxs">
        Digitalized by NYPL with permission of {publisher}
      </Text>
    </Flex>
  );
};

export default UpBlurb;
