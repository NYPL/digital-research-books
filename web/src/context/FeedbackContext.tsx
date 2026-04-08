import React, { createContext, useState } from "react";

import { ChakraComponent } from "@chakra-ui/react";
import { useFeedbackBox } from "@nypl/design-system-react-components";
import { BUTTON_DESCRIPTION_TEXT } from "../constants/feedback";

type FeedbackContextType = {
  onOpen: () => void;
  FeedbackBox: ChakraComponent<any>;
  onClose: () => void;
  isOpen?: boolean;
  isError: boolean | null;
  setIsError: React.Dispatch<React.SetStateAction<boolean | null>>;
  notificationText: string | null;
  setNotificationText: React.Dispatch<React.SetStateAction<string | null>>;
  statusCode: number | null;
  setStatusCode: React.Dispatch<React.SetStateAction<number | null>>;
  descriptionText?: string;
  setDescriptionText?: React.Dispatch<React.SetStateAction<string>>;
  thumbValue?: "up" | "down" | null;
  setThumbValue?: React.Dispatch<React.SetStateAction<"up" | "down" | null>>;
  sessionId?: string;
  setSessionId?: React.Dispatch<React.SetStateAction<string>>;
};

export const FeedbackContext = createContext<FeedbackContextType | undefined>(
  undefined
);

export const FeedbackProvider: React.FC<{
  children?: React.ReactNode;
}> = ({ children }) => {
  const { FeedbackBox, isOpen, onOpen, onClose } = useFeedbackBox();
  const [isError, setIsError] = useState(null);
  const [notificationText, setNotificationText] = useState(null);
  const [statusCode, setStatusCode] = useState(null);
  const [descriptionText, setDescriptionText] = useState(
    BUTTON_DESCRIPTION_TEXT
  );
  const [thumbValue, setThumbValue] = useState<"up" | "down" | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);

  return (
    <FeedbackContext.Provider
      value={{
        onOpen,
        FeedbackBox,
        isOpen,
        onClose,
        isError,
        setIsError,
        notificationText,
        setNotificationText,
        statusCode,
        setStatusCode,
        descriptionText,
        setDescriptionText,
        thumbValue,
        setThumbValue,
        sessionId,
        setSessionId,
      }}
    >
      {children}
    </FeedbackContext.Provider>
  );
};
