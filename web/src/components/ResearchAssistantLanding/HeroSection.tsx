import { Box, Heading, Text } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../ResearchAssistant/ResearchAssistantIcon";
import SearchSection from "./SearchSection";

const HeroSection: React.FC = () => (
    <Box backgroundColor="section.research.primary" paddingX="xs" id="hero-section">
        <Box
            display="flex"
            flexDir="column"
            paddingY="xxl"
            margin="0 auto"
            maxWidth="1280px"
            width="100%"
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
                >
                    <Box display="flex" gap="s" alignItems="center">
                        <Text noSpace>Introducing the NYPL Virtual Research Assistant</Text>
                        <Box display="inline">
                            <ResearchAssistantIcon inCircle={true} />
                        </Box>
                    </Box>
                </Heading>
                {/* TODO: use Heading subtitle prop DS v4*/}
                <Text size="subtitle1">
                    Your AI partner in discovering relevant research from over 1 million
                    scholarly e-books
                </Text>
            </Box>
            <SearchSection />
        </Box>
    </Box>
);

export default HeroSection;
