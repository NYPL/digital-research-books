import React, { useEffect, useRef, useState } from "react";
import {
  Box,
  Button,
  Form,
  TextInput,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";

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
      paddingLeft="l"
      paddingRight="xxxl"
      paddingY="s"
      // @ts-expect-error: Override gap value type
      gap="0"
    >
      {messages.length > 0 && (
        <Box display="flex" flexDir="row-reverse" gap="xs" marginBottom="xs">
          {[1, 2, 3].map((item) => (
            <Button
              id={`related-item-btn-${item}`}
              bgColor="ui.white"
              color="section.research.secondary"
              borderColor="section.research.secondary"
              buttonType="secondary"
              size="small"
              key={item}
              _hover={{
                bgColor: "#f3f7fc",
              }}
            >
              Lorem ipsum
            </Button>
          ))}
        </Box>
      )}
      <Box display="flex" flexDir="row" alignItems="center" gap="0">
        <TextInput
          autoComplete="off"
          disabled={isDisabled}
          flex="1"
          id="chat-input"
          labelText={""}
          height="fit-content"
          minHeight="40px"
          onChange={(e) => setInputText(e.target.value)}
          onInput={(e) => {
            const target = e.target as HTMLTextAreaElement;
            target.style.height = "0px";
            target.style.height = target.scrollHeight + "px";
          }}
          placeholder={placeholderValue}
          ref={inputRef}
          type="textarea"
          value={inputText}
          sx={{
            textarea: {
              height: "40px",
              minHeight: "40px",
              maxHeight: "80px",
              resize: "none"
            },
          }}
        />
        <Button type="submit" isDisabled={isDisabled} id="send-chat-button">
          Send
        </Button>
      </Box>
    </Form>
  );
};

export default ResearchAssistantInput;
