import { Flex, Heading, Icon } from "@nypl/design-system-react-components";
import React from "react";

interface EmptySearchPromptProps {
  message?: string;
  [styleProp: string]: any; // for ds styling props
}

const EmptySearchPrompt: React.FC<EmptySearchPromptProps> = ({
  message = "Start searching to see results from over 1 million Digitized Research Books",
  ...rest
}) => (
  <Flex
    gap="s"
    bgColor="ui.bg.default"
    alignItems="center"
    paddingY="xxl"
    {...rest}
  >
    <Flex
      alignItems="center"
      flex="1"
      flexDir="column"
      gap="xs"
      height="100%"
      margin="0 auto"
      maxWidth="1280px"
    >
      <Icon color="section.research.secondary" name="search" size="xlarge" />
      <Heading
        color="section.research.secondary"
        size="heading6"
        textAlign="center"
        maxWidth="600px"
      >
        {message}
      </Heading>
    </Flex>
  </Flex>
);

export default EmptySearchPrompt;
