import React, { useState } from "react";
import {
  Box,
  Button,
  Form,
  TextInput,
} from "@nypl/design-system-react-components";
import { Message } from "./useResearchAssistant";

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

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputText.trim() && !isDisabled) {
      onSendMessage(inputText);
      setInputText("");
    }
  };

  const placeholderValue = isDisabled ? "Assistant is thinking..." : messages.length === 0 ? "Ask your question..." : "Enter your response here"

  return (
    <Form onSubmit={handleSubmit} id="research-assistant-form">
      <Box display="flex" flexDir="row">
        <TextInput
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          placeholder={placeholderValue}
          disabled={isDisabled}
          id="chat-input"
          labelText={""}
          flex="1"
        />
        <Button
          type="submit"
          isDisabled={isDisabled}
          id="send-chat-button"
        >
          Send
        </Button>
      </Box>
    </Form>
  );
};

export default ResearchAssistantInput;
