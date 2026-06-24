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
        paddingBottom={{ base: "s", md: "20px" }}
        ref={ref}
        background="linear-gradient(0deg, #FFF 0%, #FFFEF9 100%)"
      >
        <Box
          display="flex"
          flexDir="column"
          alignItems="center"
          marginBottom={{
            base: "l",
            md: "xxl",
          }}
        >
          <Heading
            level="h1"
            fontWeight="bold"
            marginBottom="s"
            fontFamily="Domine"
            fontSize={{
              base: "mobile.heading.heading2",
              md: "desktop.heading.heading1",
            }}
            lineHeight={{
              base: "125%",
              md: "115%",
            }}
          >
            Try our new AI-enabled Enhanced Search
          </Heading>
          <Text
            color="ui.gray.x-dark"
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading4",
            }}
            lineHeight={{
              base: "140%",
              md: "130%",
            }}
            maxWidth={{
              base: "480px",
              md: "none",
            }}
            fontWeight={{
              base: "semibold",
              md: "medium",
            }}
          >
            Find and discover content using natural language. Now live in
            Digitized Research Books.
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
