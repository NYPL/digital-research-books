import { Heading, Hero } from "@nypl/design-system-react-components";
import React from "react";

export const DrbHero: React.FC = () => {
  return (
    <Hero
      backgroundColor="section.research.primary"
      variant="tertiary"
      heading={
        <Heading level="h1" id="tertiary-hero">
          Digital Research Books <sup>Beta</sup>
        </Heading>
      }
    />
  );
};

export default DrbHero;
