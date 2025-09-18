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
  const [inputText, setInputText] = useState("");
  const inputRef = useRef<TextInputRefType>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputText.trim() && !isDisabled) {
      onSendMessage(inputText);
      setInputText("");

      if (inputRef.current && !isDisabled) {
        inputRef.current.focus();
      }
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
      onSubmit={handleSubmit}
      id="research-assistant-form"
      borderTop="1px white solid"
      paddingX="l"
      paddingY="s"
      sx={{
        "#research-assistant-form-parent": {
          gap: 0,
        },
      }}
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
        paddingRight="s"
      >
        <TextInput
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          placeholder={placeholderValue}
          isDisabled={isDisabled}
          id="chat-input"
          autoComplete="off"
          labelText={""}
          ref={inputRef}
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
          type="submit"
          isDisabled={isDisabled}
          id="send-chat-button"
          backgroundColor="transparent"
          height="64px"
          borderRadius="8px"
          _hover={{
            backgroundColor: "ui.white",
          }}
          _disabled={{
            backgroundColor: "ui.disabled.secondary",
          }}
          aria-label="Send"
        >
          <ResearchAssistantSendIcon isDisabled={isDisabled} />
        </Button>
      </Box>
    </Form>
  );
};

export default ResearchAssistantInput;
