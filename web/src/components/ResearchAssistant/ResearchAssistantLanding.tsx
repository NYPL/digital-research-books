import React, { useState } from "react";
import {
    Box,
    Button,
    Form,
    Heading,
    TextInput,
    Text,
    Icon,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";

interface ResearchAssistantLandingProps {
    onSearchSubmit: (query: string) => void;
}

const ResearchAssistantLanding: React.FC<ResearchAssistantLandingProps> = ({
    onSearchSubmit,
}) => {
    const [searchInput, setSearchInput] = useState("");

    const handleLocalSearchSubmit = () => {
        if (searchInput.trim()) {
            onSearchSubmit(searchInput.trim());
        }
    };

    const handleSuggestionClick = (suggestion: string) => {
        onSearchSubmit(suggestion);
    };

    const featuredSuggestions = [
        "I am interested in the downfall of the Roman Empire",
        "Show me books on feminism in medieval times",
        "I want to learn about the history of the Methodist Church",
        "Research recommendations on the American Civil War",
    ];

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
        <>
            <DrbBreakout
                breadcrumbsData={[
                    { url: "/research-assistant", text: "Virtual Research Assistant" },
                ]}
            >
                <DrbHero />
                <Box height="l" paddingY="xs" display="flex" maxWidth="1280px" alignItems="center" margin="auto" gap="s">
                    <Box display="flex" alignItems="center" paddingX="s" paddingY="xxs" backgroundColor="section.research.primary-05" borderRadius="6px">
                        <ResearchAssistantIcon />
                        <Text size="body1" color="section.research.secondary" isBold noSpace>Virtual Research Assistant</Text>
                    </Box>
                    <Box display="flex" alignItems="center">
                        <Icon name="search" align="left" size="large" />
                        <Text noSpace>Keyword search</Text>
                    </Box>
                </Box>
            </DrbBreakout>

            <Box display="flex" flexDir="column">
                <Box
                    display="flex"
                    flexDir="column"
                    gap="l"
                    padding="xxxl"
                    backgroundColor="section.research.primary"
                >
                    <Box
                        display="flex"
                        flexDir="column"
                        alignItems="center"
                        color="ui.white"
                    >
                        <Heading level="h2" color="ui.white">
                            <Box display="flex" gap="s" alignItems="center">
                                <Text noSpace>
                                    Introducing the NYPL Virtual Research Assistant
                                </Text>
                                <Box display="inline">
                                    <ResearchAssistantIcon />
                                </Box>
                            </Box>
                        </Heading>
                        {/* TODO: use Heading subtitle prop DS v4*/}
                        <Text size="subtitle1">
                            Your AI partner in discovering relevant research from over 1
                            million scholarly e-books
                        </Text>
                    </Box>
                    <Form
                        id="vra-landing-form"
                        paddingY="s"
                        // @ts-expect-error: Override gap value type
                        gap="0"
                    >
                        <Box display="flex" flexDir="row" gap="0">
                            <TextInput
                                value={searchInput}
                                onChange={(e) => setSearchInput(e.target.value)}
                                placeholder="What research topic can I help you explore today?"
                                id="message-input"
                                labelText={""}
                                flex="1"
                            />
                            <Button
                                onClick={handleLocalSearchSubmit}
                                aria-label="Start research"
                                id="research-landing-submit"
                            >
                                Send
                            </Button>
                        </Box>
                    </Form>

                    <Box display="grid" gridTemplateColumns="repeat(2, 1fr)" gap="s">
                        {featuredSuggestions.map((suggestion, index) => (
                            <Button
                                key={index}
                                onClick={() => handleSuggestionClick(suggestion)}
                                id={`suggestion-${index}`}
                                buttonType="secondary"
                                backgroundColor="ui.white"
                                gap="xs"
                                alignItems="left"
                                justifyContent="flex-start"
                                _hover={{
                                    backgroundColor: "#f3f7fc",
                                }}
                            >
                                <Box display="flex" justifyContent="space-between" width="100%">
                                    <Box
                                        display="flex"
                                        alignContent="center"
                                        alignSelf="flex-start"
                                    >
                                        <ResearchAssistantIcon />
                                        <Text>{suggestion}</Text>
                                    </Box>
                                    <Icon align="right" name="search" size="large" />
                                </Box>
                            </Button>
                        ))}
                    </Box>
                    <Button
                        id="learn-more"
                        color="ui.white"
                        borderColor="ui.white"
                        marginTop="s"
                        buttonType="secondary"
                        width="200px"
                        margin="0 auto"
                    >
                        Learn more ↓
                    </Button>
                </Box>

                <Box
                    backgroundColor="ui.gray.light-cool"
                    paddingX="xxxl"
                    paddingY="l"
                    margin="0 auto"
                    color="section.research.secondary"
                    textAlign="center"
                >
                    <Heading
                        level="h2"
                        size="heading3"
                        color="section.research.secondary"
                    >
                        Get more out of your research journey with the power of AI
                    </Heading>
                    <Box
                        display="grid"
                        gridTemplateColumns="repeat(4, 1fr)"
                        gridTemplateRows="auto 1fr"
                        gap="xl"
                    >
                        {features.map((feature, index) => (
                            <Box key={index}>
                                <Heading
                                    level="h3"
                                    size="heading5"
                                    color="section.research.secondary"
                                    gridColumn={index + 1}
                                    gridRow="1"
                                >
                                    {feature.title}
                                </Heading>
                                <Box gridColumn={index + 1} gridRow="2">
                                    {feature.description}
                                </Box>
                            </Box>
                        ))}
                    </Box>
                </Box>
            </Box>
        </>
    );
};

export default ResearchAssistantLanding;
