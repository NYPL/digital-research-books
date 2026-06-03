import {
  Flex,
  Heading,
  Hero,
  Text,
} from "@nypl/design-system-react-components";
import React from "react";

export const EnhancedSearchHero: React.FC = () => {
  return (
    <Hero
      backgroundColor="section.research.primary"
      variant="tertiary"
      heading={
        <Flex>
          <Heading level="h2" fontSize="heading2" id="tertiary-hero">
            Digitized Research Books
          </Heading>
          <Text fontSize="desktop.subtitle.subtitle1" lineHeight="135%">
            A collection of scholarly books published prior to 1930, all
            available digitally to read and download for free. No library card
            required.
          </Text>
        </Flex>
      }
    />
  );
};

export default EnhancedSearchHero;
