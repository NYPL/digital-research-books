import { Box, Heading, Text } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../ResearchAssistant/ResearchAssistantIcon";
import SearchSection from "./SearchSection";
import { forwardRef } from "react";
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
        backgroundColor="section.research.primary"
        ref={ref}
    >
        <Box
            display="flex"
            flexDir="column"
            alignItems="center"
            color="ui.white"
            marginBottom="xxl"
        >
            <Heading
                level="h2"
                color="ui.white"
                fontWeight="semibold"
                size="heading2"
                marginBottom="s"
            >
                <Box display="flex" gap="s" alignItems="center">
                    <span>Introducing the NYPL Virtual Research Assistant</span>
                    <span>
                        <ResearchAssistantIcon inCircle={true} />
                    </span>
                </Box>
            </Heading>
            {/* TODO: use Heading subtitle prop DS v4*/}
            <Text size="subtitle1">
                Your AI partner in discovering relevant research from over 1 million
                scholarly e-books
            </Text>
        </Box>
        <SearchSection helpSectionRef={helpSectionRef} />
    </SectionContainer>
));

HeroSection.displayName = "HeroSection";

export default HeroSection;
