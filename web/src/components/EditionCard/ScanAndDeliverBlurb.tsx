import { Flex, Icon, Text } from "@nypl/design-system-react-components";
import React from "react";
import Link from "../Link/Link";
import { SCAN_AND_DELIVER_LINK } from "~/src/constants/links";

const ScanAndDeliverBlurb: React.FC = () => {
  return (
    <Flex
      alignItems={{ base: "start", md: "center" }}
      marginTop={{ base: "s", md: "m" }}
      marginBottom={{ base: "s", lg: "0" }}
    >
      <Icon name="errorOutline" size="small" />
      <Text size="caption" marginLeft="xxs">
        A partial scan of this edition can be requested via NYPL&apos;s{" "}
        <Link to={SCAN_AND_DELIVER_LINK}>Scan and Deliver</Link> service
      </Text>
    </Flex>
  );
};

export default ScanAndDeliverBlurb;
