import {
    Form,
    Box,
    TextInput,
    Button,
    Icon,
    Flex,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import { useState } from "react";
import { useRouter } from "next/router";
import ResearchAssistantSendIcon from "../ResearchAssistant/icons/ResearchAssistantSendIcon";

interface SearchSectionProps {
    helpSectionRef: React.RefObject<HTMLDivElement>;
}

const SearchSection: React.FC<SearchSectionProps> = ({ helpSectionRef }) => {
    const router = useRouter();
    const [searchInput, setSearchInput] = useState("");

    const onSubmit = (query: string) => {
        sessionStorage.setItem("researchAssistantInitialMessage", query);
        router.push("/research-assistant");
    };

    const handleLocalSearchSubmit = () => {
        if (searchInput) {
            onSubmit(searchInput);
        }
    };

    const handleSuggestionClick = (suggestion: string) => {
        onSubmit(suggestion);
    };

    const featuredSuggestions = [
        "I am interested in the downfall of the Roman Empire",
        "Show me books on feminism in medieval times",
        "I want to learn about the history of the Methodist Church",
        "Research recommendations on the American Civil War",
    ];
    return (
        <>
            <Form
                id="vra-landing-form"
                marginBottom="l"
                // @ts-expect-error: Override gap value type
                gap="0"
            >
                <Box
                    display="flex"
                    flexDir="row"
                    gap="0"
                    border="1px solid"
                    borderColor="ui.border.default"
                    borderRadius="8px"
                    backgroundColor="ui.white"
                    paddingRight="xs"
                >
                    <TextInput
                        value={searchInput}
                        onChange={(e) => setSearchInput(e.target.value.trim())}
                        placeholder="What research topic can I help you explore today?"
                        id="message-input"
                        labelText={""}
                        flex="1"
                        sx={{
                            input: {
                                border: "none",
                                borderRadius: "8px",
                                height: "64px",
                                flexGrow: 2,
                            },
                        }}
                    />
                    <Button
                        onClick={handleLocalSearchSubmit}
                        id="research-landing-submit"
                        backgroundColor="transparent"
                        height="64px"
                        borderRadius="8px"
                        _hover={{
                            backgroundColor: "ui.white",
                        }}
                        aria-label="Send"
                    >
                        <ResearchAssistantSendIcon />
                    </Button>
                </Box>
            </Form>

            <Box
                display="grid"
                gridTemplateColumns="repeat(2, 1fr)"
                gap="s"
                marginBottom="xxxl"
            >
                {featuredSuggestions.map((suggestion, index) => (
                    <Button
                        key={index}
                        onClick={() => handleSuggestionClick(suggestion)}
                        id={`suggestion-${index}`}
                        variant="secondary"
                        color="section.research.secondary"
                        backgroundColor="ui.white"
                        gap="xs"
                        alignItems="left"
                        justifyContent="flex-start"
                        textAlign="left"
                        height="auto"
                        _hover={{
                            backgroundColor: "#f3f7fc",
                        }}
                    >
                        <Box
                            display="flex"
                            justifyContent="space-between"
                            alignItems="center"
                            width="100%"
                        >
                            <Flex alignItems="center" alignSelf="flex-start" gap="xxs">
                                <ResearchAssistantIcon />
                                <span>{suggestion}</span>
                            </Flex>
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
                variant="secondary"
                margin="0 auto"
                borderWidth="2px"
                onClick={() =>
                    helpSectionRef.current?.scrollIntoView({ behavior: "smooth" })
                }
            >
                Learn more ↓
            </Button>
        </>
    );
};

export default SearchSection;
