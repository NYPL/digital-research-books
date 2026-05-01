import {
  Box,
  Button,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import ArrowIcon from "../../ResearchAssistant/icons/ArrowIcon";
import SectionContainer from "../SectionContainer";
import StatisticCard from "./StatisticCard";

interface HeroSectionProps {
  missionSectionRef: React.RefObject<HTMLDivElement>;
}

const HeroSection: React.ForwardRefExoticComponent<
  HeroSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HeroSectionProps>(
  ({ missionSectionRef }, ref) => {
    const handleReadMoreClick = () => {
      missionSectionRef.current?.scrollIntoView({ behavior: "smooth" });
      window.addEventListener(
        "scrollend",
        () => missionSectionRef.current?.focus(),
        {
          once: true,
        }
      );
    };

    return (
      <SectionContainer
        display="flex"
        flexDir="column"
        background="linear-gradient(0deg, #FFF 0%, #FFFEF9 100%)"
        paddingTop="5.25rem"
        paddingBottom="l"
        ref={ref}
      >
        {/* used maxWidth here */}
        <Box maxWidth="821px" margin="0 auto">
          <Box display="flex" flexDir="column" alignItems="center">
            <Heading
              level="h1"
              fontWeight="bold"
              marginBottom="87.5px"
              size="heading1"
              fontFamily="Domine"
            >
              AI is more{" "}
              <Text as="span" color="section.research.secondary">
                meaningful
              </Text>{" "}
              <br />
              when it helps unlock access to knowledge
            </Heading>
          </Box>
          <StatisticCard />
          <Text
            fontSize="md"
            color="ui.gray.x-dark"
            marginBottom="l"
            lineHeight="150%"
            isItalic={true}
          >
            Our collection of Digitized Research Books as of 1 July 2026
            accessible through Enhanced Search
          </Text>
          <Button
            id="read-more"
            variant="secondary"
            color="section.research.secondary"
            background="transparent"
            fontSize="desktop.body.body1"
            fontWeight="medium"
            border="0"
            borderRadius="8px"
            margin="0 auto"
            onClick={handleReadMoreClick}
            _hover={{
              backgroundColor: "section.research.primary-05",
            }}
          >
            Read More? <ArrowIcon direction="down" color="#006166" />
          </Button>
        </Box>
      </SectionContainer>
    );
  }
);

HeroSection.displayName = "HeroSection";

export default HeroSection;
