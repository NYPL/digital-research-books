import { Box, Heading } from "@nypl/design-system-react-components";
import SectionContainer from "./SectionContainer";

const FocusedResearchSection: React.FC = () => {
    return (
        <SectionContainer
            backgroundColor="ui.bg.default"
            color="section.research.secondary"
        >
            <Heading
                level="h2"
                size="heading3"
                fontWeight="semibold"
                color="section.research.secondary"
                marginBottom="l"
            >
                Do focused research with smart tools
            </Heading>
            <Box
                width="1280px"
                height="580px"
                backgroundColor="section.research.secondary"
                borderRadius="8px"
            ></Box>
        </SectionContainer>
    );
};

export default FocusedResearchSection;
