import { Box, Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import SearchSection from "./SearchSection";
import SectionContainer from "./SectionContainer";

interface HeroSectionProps {
  helpSectionRef: React.RefObject<HTMLDivElement>;
}

const HeroSection: React.ForwardRefExoticComponent<
  HeroSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HeroSectionProps>(({ helpSectionRef }, ref) => (
  <SectionContainer
    display="flex"
    flexDir="column"
    ref={ref}
    background="linear-gradient(0deg, #FFF 0%, #FFFEF9 100%)"
  >
    <Box display="flex" flexDir="column" alignItems="center" marginBottom="xxl">
      <Heading level="h1" fontWeight="bold" marginBottom="s" size="heading1">
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
    <SearchSection helpSectionRef={helpSectionRef} />
  </SectionContainer>
));

HeroSection.displayName = "HeroSection";

export default HeroSection;
