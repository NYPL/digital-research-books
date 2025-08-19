import { Box, Heading } from "@nypl/design-system-react-components";

const FocusedResearchSection: React.FC = () => {
    return (
        <Box backgroundColor="ui.bg.default">
            <Box
                paddingY="xxl"
                color="section.research.secondary"
                textAlign="center"
                margin="0 auto"
                maxWidth="1280px"
                width="100%"
            >
                <Heading
                    level="h2"
                    size="heading3"
                    fontWeight="semibold"
                    color="section.research.secondary"
                    marginBottom="l"
                >
                    Do focused research with snart tools
                </Heading>
                <Box
                    width="1280px"
                    height="580px"
                    backgroundColor="section.research.secondary"
                    borderRadius="8px"
                ></Box>
            </Box>
        </Box>
    );
};

export default FocusedResearchSection;
