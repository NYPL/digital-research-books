import {
  Box,
  Heading,
  Hero,
  Template,
  TemplateBreakout,
  TemplateContent,
  TemplateMain,
  useNYPLBreakpoints,
} from "@nypl/design-system-react-components";
import React from "react";
import SearchForm from "~/src/components/SearchForm/SearchForm";
import { Opds2Feed } from "~/src/types/OpdsModel";
import CollectionList from "../CollectionList/CollectionList";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import Link from "../Link/Link";

const LandingPage: React.FC<{ collections?: Opds2Feed }> = ({
  collections,
}) => {
  const subHeader = (
    <Box
      sx={{
        a: {
          color: "ui.link.primary",
          display: "inline",
        },
      }}
    >
      <span>
        Find millions of digital books for research from multiple sources
        world-wide--all free to read, download, and keep. No library card
        required. This is an early beta test, so we want your feedback!{" "}
        <Link to="/about">Read more about the project</Link>.
      </span>
      <Box marginTop="s">
        <SearchForm />
      </Box>
    </Box>
  );

  const {
    isLargerThanMedium,
    isLargerThanMobile,
    isLargerThanLarge,
    isLargerThanXLarge,
  } = useNYPLBreakpoints();

  let backgroundImageSrc =
    "https://drb-files-qa.s3.amazonaws.com/hero/heroDesktop2x.jpg";
  if (isLargerThanXLarge) {
    backgroundImageSrc =
      "https://drb-files-qa.s3.amazonaws.com/hero/heroDesktop2x.jpg";
  } else if (isLargerThanLarge) {
    backgroundImageSrc =
      "https://drb-files-qa.s3.amazonaws.com/hero/heroDesktop.jpg";
  } else if (isLargerThanMedium) {
    backgroundImageSrc =
      "https://drb-files-qa.s3.amazonaws.com/hero/heroTabletLarge.jpg";
  } else if (isLargerThanMobile) {
    backgroundImageSrc =
      "https://drb-files-qa.s3.amazonaws.com/hero/heroTabletSmall.jpg";
  } else {
    backgroundImageSrc =
      "https://drb-files-qa.s3.amazonaws.com/hero/heroMobile.jpg";
  }

  const breakoutElement = (
    <DrbBreakout>
      <DrbHero />
      <Hero
        textBackgroundColor="ui.gray.light-cool"
        backgroundImageSrc={backgroundImageSrc}
        textColor="ui.black"
        variant="primary"
        heading={
          <Heading
            level="h1"
            size="heading2"
            id="primary-hero"
            color="ui.black"
            marginBottom="s"
          >
            Search the World&apos;s Research Collections
          </Heading>
        }
        subHeaderText={subHeader}
      />
    </DrbBreakout>
  );

  const contentElement = (
    <Box marginLeft="l" marginRight="l">
      <Heading level="h2" marginBottom="s">
        Recently Added Collections
      </Heading>
      <CollectionList collections={collections} />
    </Box>
  );
  return (
    <Template>
      <TemplateBreakout>{breakoutElement}</TemplateBreakout>
      <TemplateMain paddingBottom="l">
        <TemplateContent>{contentElement}</TemplateContent>
      </TemplateMain>
    </Template>
  );
};

export default LandingPage;
