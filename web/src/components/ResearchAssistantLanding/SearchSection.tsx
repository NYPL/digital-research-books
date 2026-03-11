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
import { useRef, useState } from "react";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import ResearchAssistantSendIcon from "../ResearchAssistant/icons/ResearchAssistantSendIcon";

interface SearchSectionProps {
  featuresSectionRef: React.RefObject<HTMLDivElement>;
}

const SearchSection: React.FC<SearchSectionProps> = ({
  featuresSectionRef,
}) => {
  const router = useRouter();
  const [searchInput, setSearchInput] = useState("");

  const [isFocused, setIsFocused] = useState(false);
  const inputRef = useRef<TextInputRefType>(null);

  const onSubmit = (query: string) => {
    sessionStorage.setItem("researchAssistantInitialMessage", query);
    router.push("/research-assistant");
  };

  const handleLocalSearchSubmit = () => {
    if (searchInput.trim()) {
      onSubmit(searchInput.trim());
    }
  };

  const handleClearSearch = () => {
    setSearchInput("");
    inputRef.current.value = "";
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
          borderColor="section.research.primary-10"
          borderRadius="8px"
          backgroundColor="ui.white"
          padding="0.5rem 1.5rem"
          sx={
            isFocused
              ? {
                  boxShadow: "none",
                  outline: "2px solid",
                  outlineColor: "section.research.secondary",
                }
              : {}
          }
        >
          <TextInput
            autoComplete="off"
            id="chat-input"
            ref={inputRef}
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
            flex="1"
            height="fit-content"
            minHeight="92px"
            color="ui.typography.body"
            sx={{
              textarea: {
                border: "none",
                height: "92px",
                minHeight: "92px",
                resize: "none",
                paddingY: "s",
                paddingX: "0",
              },
              "textarea:focus": {
                outline: "none !important",
                boxShadow: "none !important",
              },
            }}
          />
          <Flex flexDir="row" gap="xs" paddingY="s">
            {searchInput !== "" && (
              <Button
                onClick={handleClearSearch}
                id="research-landing-clear"
                backgroundColor="transparent"
                height="1.5rem"
                minWidth="1.5rem"
                borderRadius="8px"
                _hover={{
                  backgroundColor: "section.research.primary-10",
                }}
                aria-label="Clear"
                padding="0"
              >
                <Icon name="close" size="large" color="ui.black" />
              </Button>
            )}
            <Button
              onClick={handleLocalSearchSubmit}
              id="research-landing-submit"
              backgroundColor="transparent"
              height="1.5rem"
              minWidth="1.5rem"
              borderRadius="8px"
              _hover={{
                backgroundColor: "section.research.primary-10",
              }}
              aria-label="Send"
              padding="0"
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
            onClick={() => handleSuggestionClick(suggestion)}
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
              <Icon align="right" name="search" size="large" />
            </Box>
          </Button>
        ))}
      </Box>
      <Button
        id="how-does-it-work"
        marginTop="s"
        variant="secondary"
        color="section.research.secondary"
        background="transparent"
        fontSize="desktop.body.body1"
        fontWeight="medium"
        border="0"
        borderRadius="8px"
        margin="0 auto"
        onClick={() =>
          featuresSectionRef.current?.scrollIntoView({ behavior: "smooth" })
        }
        _hover={{
          backgroundColor: "section.research.primary-05",
        }}
      >
        How does it work? <ArrowIcon direction="down" color="#006166" />
      </Button>
    </>
  );
};

export default SearchSection;
