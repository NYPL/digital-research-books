import { Text } from "@nypl/design-system-react-components";
import React from "react";

interface AiGeneratedTextProps {
  isInitial?: boolean;
}

const AiGeneratedText: React.FC<AiGeneratedTextProps> = ({ isInitial }) => {
  return (
    <Text fontSize="desktop.caption" fontWeight="medium" color="ui.gray.dark">
        {isInitial ? "AI-generated." : "AI-generated. Verify results."}
    </Text>
  );
};

export default AiGeneratedText;


