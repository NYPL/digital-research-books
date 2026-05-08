import {
  Box,
  Button,
  Flex,
  Form,
  Icon,
  TextInput,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import { useState } from "react";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import ResearchAssistantSendIcon from "../ResearchAssistant/icons/ResearchAssistantSendIcon";

interface SearchSectionProps {
  textInputRef: React.RefObject<TextInputRefType>;
}

const SearchSection: React.FC<SearchSectionProps> = ({ textInputRef }) => {
  const router = useRouter();
  const [searchInput, setSearchInput] = useState("");

  const [isFocused, setIsFocused] = useState(false);

  const onSubmit = (query: string) => {
    sessionStorage.setItem("researchAssistantInitialMessage", query);
    router.push("/research-assistant");
  };

  const handleLocalSearchSubmit = () => {
    const query = searchInput.trim();

    if (query) {
      // GTM Tagging: search_query_submit
      trackEvent({
        event: "search_query_submit",
        interaction: "User Input",
        location: "Landing",
      });

      onSubmit(query);
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    onSubmit(suggestion);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      e.stopPropagation();
      handleLocalSearchSubmit();
    }
  };

  const featuredSuggestions = [
    "Political figures of Ancient Rome",
    "Ornithology in the nineteenth century",
    "Medieval warfare in China",
    "The science of shipbuilding",
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
          padding="0.5rem 1.5rem"
          sx={
            isFocused
              ? {
                  boxShadow: "none",
                  outline: "3px solid",
                  outlineColor: "section.research.secondary",
                }
              : {}
          }
        >
          <TextInput
            autoComplete="off"
            id="chat-input"
            ref={textInputRef}
            labelText={""}
            value={searchInput}
            onChange={(e) => {
              setSearchInput(e.target.value);
            }}
            onKeyDown={(e) => handleKeyDown(e)}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="What research topic can I help you explore today?"
            type="textarea"
            color="ui.typography.body"
            flex="1"
            height="fit-content"
            minHeight="92px"
            sx={{
              textarea: {
                border: "none",
                height: "92px",
                minHeight: "92px",
                resize: "none",
                paddingY: "s",
                paddingX: "0",
                fontSize: "desktop.body.body1",
              },
              "textarea:focus": {
                outline: "none !important",
                boxShadow: "none !important",
              },
            }}
          />
          <Flex flexDir="row" paddingY="xs">
            <Button
              onClick={handleLocalSearchSubmit}
              id="research-landing-submit"
              backgroundColor="transparent"
              minWidth="1.5rem"
              borderRadius="8px"
              aria-label="Send"
              padding="xs"
              isDisabled={searchInput === ""}
              _disabled={{
                backgroundColor: "transparent",
              }}
              _hover={{
                backgroundColor: "section.research.primary-10",
              }}
            >
              <ResearchAssistantSendIcon isDisabled={searchInput === ""} />
            </Button>
          </Flex>
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
            onClick={() => {
              handleSuggestionClick(suggestion);
              // GTM Tagging: prompt_suggestion_click
              trackEvent({
                event: "prompt_suggestion_click",
                interaction: "Click",
                element_id: `suggestion-${index}`,
              });
            }}
            id={`suggestion-${index}`}
            variant="secondary"
            color="section.research.secondary"
            background="section.research.primary-05"
            borderColor="section.research.primary-10"
            borderRadius="8px"
            fontWeight="medium"
            gap="xs"
            alignItems="left"
            justifyContent="flex-start"
            textAlign="left"
            height="auto"
            _hover={{
              backgroundColor: "section.research.primary-10",
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
              <Icon align="right" name="search" size="medium" />
            </Box>
          </Button>
        ))}
      </Box>
    </>
  );
};

export default SearchSection;
