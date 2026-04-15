import {
  Box,
  Button,
  ButtonGroup,
  Flex,
  Form,
  Icon,
  Text,
  TextInput,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React from "react";
import {
  CHARACTER_LIMIT,
  SURVEY_DESCRIPTION,
  SURVEY_QUESTIONS,
} from "~/src/constants/feedback";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { submitVRAPopupSurvey } from "~/src/lib/api/FeedbackApi";

const VRAPopupSurvey: React.FC = () => {
  const { isSurveyVisible, markSurveyHandled } = useResearchAssistant();
  const { sessionId } = React.useContext(FeedbackContext);
  const router = useRouter();

  const [currentQuestionIndex, setCurrentQuestionIndex] = React.useState(0);
  const [responses, setResponses] = React.useState<string[]>(
    Array(SURVEY_QUESTIONS.length).fill("")
  );
  const [isConfirmation, setIsConfirmation] = React.useState(false);

  const handleResponse = React.useCallback(
    (questionIndex: number, value: string) => {
      setResponses((prev) => {
        const newResponses = [...prev];
        newResponses[questionIndex] = value;
        return newResponses;
      });
    },
    []
  );

  const closeSurvey = React.useCallback(() => {
    markSurveyHandled();
    setIsConfirmation(false);
  }, [markSurveyHandled]);

  const onSubmit = () => {
    markSurveyHandled();
    submitVRAPopupSurvey({
      responses,
      sessionId: sessionId ?? "",
      url: router.asPath,
    });
    setIsConfirmation(true);
  };

  return (
    <Form
      position="fixed"
      bottom="48px"
      right="8px"
      background="ui.white"
      borderRadius="8px"
      border="1px solid"
      borderColor="ui.border.default"
      display={isSurveyVisible || isConfirmation ? "block" : "none"}
      gap="grid.xxs"
      padding="m"
      width="366px"
      zIndex="9999"
    >
      {SURVEY_QUESTIONS.map((question, index) => (
        <Box
          key={index}
          display={index === currentQuestionIndex ? "block" : "none"}
        >
          <Flex justifyContent="space-between">
            <Text
              size="caption"
              color="ui.gray.dark"
              visibility={isConfirmation ? "hidden" : "visible"}
            >
              {index + 1} of {SURVEY_QUESTIONS.length}
            </Text>
            <Button
              variant="iconOnly"
              aria-label="Close"
              border="none"
              color="ui.black"
              height="fit-content"
              minWidth="24px"
              padding="0"
              onClick={closeSurvey}
            >
              <Icon name="close" size="large" />
            </Button>
          </Flex>
          <Box display={isConfirmation ? "none" : "block"} marginTop="xs">
            <Flex flexDir="column" gap="xs">
              <Text size="caption">{SURVEY_DESCRIPTION}</Text>
              <Text size="body2" isBold>
                {question}
              </Text>
            </Flex>
            <Flex
              flexDir="column"
              gap="xs"
              marginY="m"
              display={index < SURVEY_QUESTIONS.length - 1 ? "flex" : "none"}
            >
              <ButtonGroup display="flex" sx={{ width: "100% !important" }}>
                {[1, 2, 3, 4, 5].map((value) => (
                  <Button
                    key={value}
                    variant={
                      responses[index] === String(value)
                        ? "primary"
                        : "secondary"
                    }
                    flex="1 0 0"
                    borderColor="ui.border.default"
                    color={
                      responses[index] === String(value)
                        ? "ui.white"
                        : "ui.typography.body"
                    }
                    onClick={() => handleResponse(index, String(value))}
                  >
                    {value}
                  </Button>
                ))}
              </ButtonGroup>
              <Flex justifyContent="space-between">
                <Text size="caption">Strongly disagree</Text>
                <Text size="caption">Strongly agree</Text>
              </Flex>
            </Flex>
            <Box
              marginY="m"
              display={index === SURVEY_QUESTIONS.length - 1 ? "block" : "none"}
            >
              <TextInput
                width="100%"
                type="textarea"
                labelText="Comment"
                value={responses[index]}
                helperText={`${
                  CHARACTER_LIMIT - responses[index].length
                } character${
                  CHARACTER_LIMIT - responses[index].length === 1 ? "" : "s"
                } remaining`}
                maxLength={CHARACTER_LIMIT}
                placeholder="Enter your feedback here"
                onChange={(e) => handleResponse(index, e.target.value)}
              />
            </Box>
            <ButtonGroup display="flex" sx={{ width: "100% !important" }}>
              <Button
                variant="secondary"
                isDisabled={index === 0}
                alignItems="center"
                display="flex"
                flexGrow="1"
                onClick={() => setCurrentQuestionIndex((prev) => prev - 1)}
              >
                <Icon name="arrow" iconRotation="rotate90" size="xsmall" />
                Previous
              </Button>
              <Button
                variant="secondary"
                alignItems="center"
                display={index < SURVEY_QUESTIONS.length - 1 ? "flex" : "none"}
                flexGrow="1"
                onClick={() => {
                  setCurrentQuestionIndex((prev) => prev + 1);
                }}
              >
                Next
                <Icon name="arrow" iconRotation="rotate270" size="xsmall" />
              </Button>
              <Button
                variant="primary"
                display={
                  index === SURVEY_QUESTIONS.length - 1 ? "block" : "none"
                }
                flexGrow="1"
                onClick={onSubmit}
              >
                Submit
              </Button>
            </ButtonGroup>
          </Box>
        </Box>
      ))}
      <Box
        display={isConfirmation ? "block" : "none"}
        fontSize="desktop.body.body2"
        paddingBottom="l"
        textAlign="center"
      >
        <Icon
          color="ui.success.primary"
          name="actionCheckCircleFilled"
          size="large"
        />
        <Text fontWeight="medium">Thank you for your feedback.</Text>
        <Text>We look forward to improving your experience.</Text>
      </Box>
    </Form>
  );
};

export default VRAPopupSurvey;
