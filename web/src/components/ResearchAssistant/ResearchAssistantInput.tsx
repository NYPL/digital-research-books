import React, { useEffect, useRef, useState } from "react";
import {
  Box,
  Button,
  Form,
  TextInput,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";
import ResearchAssistantSendIcon from "./ResearchAssistantSendIcon";

interface ResearchAssistantInputProps {
  onSendMessage: (text: string) => void;
  isDisabled: boolean;
  messages: Message[];
}

const ResearchAssistantInput: React.FC<ResearchAssistantInputProps> = ({
  onSendMessage,
  isDisabled,
  messages,
}) => {
  const [isFocused, setIsFocused] = useState(false);
  const [inputText, setInputText] = useState("");
  const inputRef = useRef<TextInputRefType>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputText.trim() && !isDisabled) {
      onSendMessage(inputText);
      setInputText("");

      if (inputRef.current && inputRef.current) {
        inputRef.current.style.height = "1.375rem";
        inputRef.current.value = "";
      }

      if (inputRef.current && !isDisabled) {
        inputRef.current.focus();
      }
    }
  };

  const updateTextareaHeight = (e: React.FormEvent<HTMLInputElement>) => {
    const target = e.target as HTMLTextAreaElement;
    target.style.height = "0px";
    target.style.height =
      target.scrollHeight >= 132
        ? target.scrollHeight + 20 + "px"
        : target.scrollHeight + 2 + "px";
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      e.stopPropagation();
      handleSubmit(e);
    }
  };

  useEffect(() => {
    if (inputRef.current && !isDisabled) {
      inputRef.current.focus();
    }
  }, [isDisabled]);

  const placeholderValue = isDisabled
    ? "Assistant is thinking..."
    : messages.length === 0
      ? "Ask your question..."
      : "Type your response here";

  return (
    <Form
      id="research-assistant-form"
      onSubmit={handleSubmit}
      borderTop="1px white solid"
      // @ts-expect-error: Override gap value type
      gap="0"
      paddingLeft="l"
      paddingRight="xxxl"
      paddingY="s"
    >
      {/* TODO: Replace with actual related items and logic when available */}
      {messages.length > 1 && (
        <Box display="flex" justifyContent="flex-end" marginBottom="xs">
          {[1, 2, 3].map((item) => (
            <Button
              variant="secondary"
              id={`related-item-btn-${item}`}
              key={item}
              bgColor="ui.white"
              color="section.research.secondary"
              borderColor="section.research.secondary"
              size="small"
              marginLeft="xs"
              _hover={{
                bgColor: "#f3f7fc",
              }}
            >
              Related item {item}
            </Button>
          ))}
        </Box>
      )}
      <Box
        alignItems="center"
        border="1px solid"
        borderColor="ui.border.default"
        borderRadius="8px"
        backgroundColor="ui.white"
        display="flex"
        flexDir="row"
        gap="0"
        padding="s"
        sx={
          isFocused
            ? {
              boxShadow: "none",
              outline: "2px solid",
              outlineOffset: "2px",
              outlineColor: "ui.focus",
            }
            : {}
        }
      >
        <TextInput
          autoComplete="off"
          isDisabled={isDisabled}
          id="chat-input"
          labelText={""}
          onChange={(e) => setInputText(e.target.value)}
          onInput={(e) => updateTextareaHeight(e)}
          onKeyDown={(e) => handleKeyDown(e)}
          onFocus={() => setIsFocused(true)}
          onBlur={() => setIsFocused(false)}
          placeholder={placeholderValue}
          ref={inputRef}
          type="textarea"
          value={inputText}
          flex="1"
          height="fit-content"
          minHeight="1.375rem"
          sx={{
            textarea: {
              border: "none",
              height: "1.375rem",
              minHeight: "1.375rem",
              maxHeight: "132px", // 6 rows
              resize: "none",
              padding: 0,
            },
            "textarea:focus": {
              outline: "none",
              boxShadow: "none",
            },
          }}
        />
        <Button
          aria-label="Send"
          backgroundColor="transparent"
          borderRadius="8px"
          isDisabled={isDisabled || inputText === ""}
          height="24px"
          id="send-chat-button"
          padding="0"
          type="submit"
          width="24px"
          _hover={{
            backgroundColor: "ui.white",
          }}
          _disabled={{
            backgroundColor: "transparent",
          }}
        >
          <ResearchAssistantSendIcon
            isDisabled={isDisabled || inputText === ""}
          />
        </Button>
      </Box>
    </Form>
  );
};

export default ResearchAssistantInput;
