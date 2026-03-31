import {
  Box,
  Button,
  Heading,
  Text,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";
import SearchSection from "./SearchSection";
import SectionContainer from "./SectionContainer";

interface HeroSectionProps {
  featuresSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
}

const HeroSection: React.ForwardRefExoticComponent<
  HeroSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HeroSectionProps>(
  ({ featuresSectionRef, textInputRef }, ref) => {
    const handleHowDoesItWorkClick = () => {
      featuresSectionRef.current?.scrollIntoView({ behavior: "smooth" });
      window.addEventListener(
        "scrollend",
        () => featuresSectionRef.current?.focus(),
        {
          once: true,
        }
      );
    };

    return (
      <SectionContainer
        display="flex"
        flexDir="column"
        ref={ref}
        background="linear-gradient(0deg, #FFF 0%, #FFFEF9 100%)"
      >
        <Box
          display="flex"
          flexDir="column"
          alignItems="center"
          marginBottom="xxl"
        >
          <Heading
            level="h1"
            fontWeight="bold"
            marginBottom="s"
            size="heading1"
          >
            <Box display="flex" alignItems="center" fontFamily="Domine">
              <Text as="span" color="section.research.secondary">
                New!&nbsp;
              </Text>
              <span>The NYPL Virtual Research Assistant</span>
            </Box>
          </Heading>
          <Text
            fontSize="desktop.heading.heading4"
            fontWeight="medium"
            color="ui.gray.x-dark"
          >
            <span>Your AI partner in discovering content from over</span>
            <Text as="span" color="section.research.secondary">
              &nbsp;1 million digitized research books
            </Text>
          </Text>
        </Box>
        <SearchSection textInputRef={textInputRef} />
        <Button
          id="how-does-it-work"
          marginTop="s"
          variant="secondary"
          color="section.research.secondary"
          background="transparent"
          fontSize="desktop.body.body1"
          fontWeight="medium"
          border="0"
          borderRadius="8px"
          margin="0 auto"
          onClick={handleHowDoesItWorkClick}
          _hover={{
            backgroundColor: "section.research.primary-05",
          }}
        >
          How does it work? <ArrowIcon direction="down" color="#006166" />
        </Button>
      </SectionContainer>
    );
  }
);

HeroSection.displayName = "HeroSection";

export default HeroSection;
