import { FeedbackBoxViewType } from "@nypl/design-system-react-components";
import React, { useContext, useEffect, useState } from "react";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { submitDRBFeedback } from "~/src/lib/api/FeedbackApi";

const DEFAULT_DESCRIPTION_TEXT = "Please share your question or feedback.";
const ERROR_DESCRIPTION_TEXT = "We are here to help!";
const ERROR_NOTIFICATION_TEXT = `You are asking for help or information about a page error.`;

const Feedback: React.FC<any> = ({ location }) => {
  const [view, setView] = useState<FeedbackBoxViewType>("form");
  const [descriptionText, setDescriptionText] = useState(
    DEFAULT_DESCRIPTION_TEXT
  );
  const {
    FeedbackBox,
    isOpen,
    onOpen,
    onClose,
    isError,
    notificationText,
    statusCode,
    setIsError,
    setNotificationText,
  } = useContext(FeedbackContext);

  useEffect(() => {
    if (isError) {
      setDescriptionText(ERROR_DESCRIPTION_TEXT);
      setNotificationText(ERROR_NOTIFICATION_TEXT);
    } else {
      setDescriptionText(DEFAULT_DESCRIPTION_TEXT);
    }
  }, [isError, setNotificationText]);

  const onCloseAndReset = () => {
    if (isError) setIsError(false);
    if (notificationText) setNotificationText(null);
    onClose();
    setView("form");
  };

  const handleFeedbackSubmit = (
    values: React.ComponentProps<typeof FeedbackBox>["onSubmit"]
  ) => {
    submitDRBFeedback({
      feedback: isError
        ? `Error Code: ${statusCode ?? "Unknown"} - ${values.comment}`
        : values.comment,
      category: isError ? "Bug" : values.category,
      url: location,
      email: values.email,
    })
      .then((res) => {
        if (res.ok) setView("confirmation");
      })
      .catch((err) => {
        console.error(err);
        setView("error");
      });
    setView("confirmation");
  };

  return (
    <FeedbackBox
      showCategoryField={!isError}
      showEmailField={isError}
      isOpen={isOpen}
      onClose={onCloseAndReset}
      onOpen={onOpen}
      onSubmit={handleFeedbackSubmit}
      confirmationText="Thank you, your feedback has been submitted."
      descriptionText={descriptionText}
      notificationText={notificationText}
      id="feedbackBox-id"
      title="Help and Feedback"
      view={view}
    />
  );
};

export default Feedback;
