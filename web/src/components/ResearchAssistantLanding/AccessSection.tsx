import { Box, Heading, Text } from "@nypl/design-system-react-components";

const AccessSection: React.FC = () => {
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
        <Box backgroundColor="section.research.primary">
            <Box
                paddingY="xxl"
                color="ui.white"
                textAlign="center"
                margin="0 auto"
                maxWidth="1280px"
                width="100%"
            >
                <Heading
                    level="h2"
                    size="heading3"
                    fontWeight="semibold"
                    color="ui.white"
                    marginBottom="l"
                >
                    Access and engage with scholarly e-books in minutes
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
                            backgroundColor="section.research.secondary"
                            borderRadius="8px"
                            paddingX="s"
                            paddingBottom="s"
                        >
                            <Text
                                gridRow="2"
                                size="body1"
                                fontWeight="medium"
                                color="ui.white"
                                noSpace
                            >
                                {feature.title}
                            </Text>
                            <Box
                                gridRow="3"
                                fontSize="body2"
                                fontWeight="medium"
                                width="100%"
                                height="200px"
                                borderRadius="8px"
                                backgroundColor="rgba(0, 131, 138, 0.20)"
                            ></Box>
                        </Box>
                    ))}
                </Box>
            </Box>
        </Box>
    );
};

export default AccessSection;
