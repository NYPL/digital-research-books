import { Box, Heading } from "@nypl/design-system-react-components";

const FeaturesSection: React.FC = () => {
    const features = [
        {
            title: "Discover relevant content",
            description:
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        },
        {
            title: "Get oriented quickly",
            description:
                "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit.",
        },
        {
            title: "Find the important parts",
            description:
                "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione.",
        },
        {
            title: "Explore rich pathways",
            description:
                "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo.",
        },
    ];
    
    return (
        <Box backgroundColor="ui.bg.default" paddingX="xs" id="features-section">
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
                    Get more out of your research journey with the power of AI
                </Heading>
                <Box
                    display="grid"
                    gridTemplateColumns="repeat(4, 1fr)"
                    gridTemplateRows="auto"
                    gap="xl"
                >
                    {features.map((feature, index) => (
                        <Box
                            key={index}
                            gridColumn={index + 1}
                            display="grid"
                            gridTemplateColumns="1fr"
                            gridTemplateRows="auto auto 1fr"
                            justifyItems="center"
                            textAlign="center"
                            gap="s"
                        >
                            <Box
                                gridRow="1"
                                borderRadius="100"
                                width="48px"
                                height="48px"
                                backgroundColor="section.research.primary"
                            />
                            <Heading
                                gridRow="2"
                                level="h3"
                                size="heading5"
                                color="section.research.secondary"
                                noSpace
                            >
                                {feature.title}
                            </Heading>
                            <Box gridRow="3" fontSize="body2" fontWeight="medium">
                                {feature.description}
                            </Box>
                        </Box>
                    ))}
                </Box>
            </Box>
        </Box>
    );
};

export default FeaturesSection;
