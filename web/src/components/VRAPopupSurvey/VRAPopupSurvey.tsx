import {
  Box,
  Button,
  ButtonGroup,
  Flex,
  FocusLock,
  Form,
  Heading,
  Icon,
  Radio,
  RadioGroup,
  Text,
  TextInput,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useContext, useEffect, useRef, useState } from "react";
import HiddenAria from "~/src/components/HiddenAria/HiddenAria";
import {
  ANNOUNCE_THRESHOLDS,
  CHARACTER_LIMIT,
  SURVEY_DESCRIPTION,
  SURVEY_QUESTIONS,
} from "~/src/constants/feedback";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { submitVRAPopupSurvey } from "~/src/lib/api/FeedbackApi";

const VRAPopupSurvey: React.FC = () => {
  const { isSurveyVisible, markSurveyHandled } = useResearchAssistant();
  const { sessionId } = useContext(FeedbackContext);
  const router = useRouter();

  const [currentQuestionIndex, setCurrentQuestionIndex] = useState(0);
  const [responses, setResponses] = useState<string[]>(
    Array(SURVEY_QUESTIONS.length).fill("")
  );
  const [isConfirmation, setIsConfirmation] = useState(false);
  const [charCountAnnouncement, setCharCountAnnouncement] = useState("");

  const dialogRef = useRef<HTMLDivElement & HTMLFormElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const questionRef = useRef<HTMLDivElement>(null);
  const confirmationRef = useRef<HTMLDivElement>(null);
  const announcedThresholdsRef = useRef<Set<number>>(new Set());
  const previousFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (isSurveyVisible && headingRef.current) {
      previousFocusRef.current = document.activeElement as HTMLElement;
    }
  }, [isSurveyVisible]);

  useEffect(() => {
    if (currentQuestionIndex >= 0 && questionRef.current) {
      questionRef.current.focus();
    }
  }, [currentQuestionIndex]);

  useEffect(() => {
    if (isConfirmation && confirmationRef.current) {
      confirmationRef.current.focus();
    }
  }, [isConfirmation]);

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
    setCurrentQuestionIndex(0);
    setResponses(Array(SURVEY_QUESTIONS.length).fill(""));
    setCharCountAnnouncement("");
    announcedThresholdsRef.current.clear();
    previousFocusRef.current?.focus();
    previousFocusRef.current = null;
  }, [markSurveyHandled]);

  const onSubmit = () => {
    submitVRAPopupSurvey({
      responses,
      sessionId: sessionId ?? "",
      url: router.asPath,
    });
    setIsConfirmation(true);
  };

  const lastQuestionIndex = SURVEY_QUESTIONS.length - 1;
  const isLastQuestion = currentQuestionIndex === lastQuestionIndex;
  const remainingChars = CHARACTER_LIMIT - responses[lastQuestionIndex].length;

  if (!isSurveyVisible && !isConfirmation) return null;

  return (
    <FocusLock initialFocusRef={headingRef}>
      <Form
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label="Enhanced search popup survey"
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
        <HiddenAria>{charCountAnnouncement}</HiddenAria>

        {!isConfirmation && (
          <>
            <Flex justifyContent="space-between" alignItems="flex-start">
              <Heading
                ref={headingRef}
                id="survey-heading"
                level="h2"
                size="heading7"
                tabIndex={-1}
                style={{ outline: "none" }}
              >
                Enhanced Search Survey
              </Heading>
              <Button
                variant="iconOnly"
                aria-label="Close survey"
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
            <Text size="caption" color="ui.gray.dark">
              <HiddenAria>
                Question {currentQuestionIndex + 1} of {SURVEY_QUESTIONS.length}
              </HiddenAria>
              <span aria-hidden="true">
                {currentQuestionIndex + 1} of {SURVEY_QUESTIONS.length}
              </span>
            </Text>
            <Box marginTop="xs">
              <Flex flexDir="column" gap="xs">
                {!isLastQuestion && (
                  <Text size="caption">{SURVEY_DESCRIPTION}</Text>
                )}
                <Text
                  ref={questionRef}
                  id={`q-${currentQuestionIndex}`}
                  size="body2"
                  fontWeight="medium"
                  tabIndex={-1}
                  sx={{ "&:focus": { outline: "none" } }}
                >
                  {SURVEY_QUESTIONS[currentQuestionIndex]}
                  {!isLastQuestion && (
                    <HiddenAria>
                      (1 = Strongly disagree, 5 = Strongly agree)
                    </HiddenAria>
                  )}
                </Text>
              </Flex>

              {!isLastQuestion ? (
                <Box marginY="m">
                  <RadioGroup
                    key={`group-${currentQuestionIndex}`}
                    id={`group-${currentQuestionIndex}`}
                    labelText={SURVEY_QUESTIONS[currentQuestionIndex]}
                    showLabel={false}
                    name={`survey-q-${currentQuestionIndex}`}
                    defaultValue={responses[currentQuestionIndex]}
                    onChange={(val: string) =>
                      handleResponse(currentQuestionIndex, val)
                    }
                    layout="row"
                    isFullWidth
                    sx={{
                      ".chakra-radio__control": { display: "none" },
                      ".chakra-radio__label": {
                        width: "100%",
                        textAlign: "center",
                        border: "1px solid",
                        borderColor: "ui.border.default",
                        borderRadius: "sm",
                        paddingX: "button.medium.px",
                        paddingY: "button.medium.py",
                        margin: "0",
                        cursor: "pointer",
                        color: "ui.typography.body",
                        fontSize: "desktop.button.default",
                        fontWeight: "button.default",
                        lineHeight: "1.5",
                      },
                      ".chakra-radio__label:hover": {
                        bg: "ui.link.primary-05",
                        borderColor: "ui.link.secondary",
                        color: "ui.link.secondary",
                      },
                      ".ds-radioGroup-stack": {
                        gap: "xs",
                        width: "100%",
                        "> div": { flex: "1 0 0" },
                      },
                      ".chakra-radio[data-checked] .chakra-radio__label": {
                        bg: "ui.link.primary",
                        borderColor: "ui.link.primary",
                        color: "ui.white",
                      },
                      ".chakra-radio:focus-within .chakra-radio__label": {
                        outline: "2px solid",
                        outlineColor: "ui.focus",
                        outlineOffset: "2px",
                      },
                    }}
                  >
                    <Radio labelText="1" value="1" />
                    <Radio labelText="2" value="2" />
                    <Radio labelText="3" value="3" />
                    <Radio labelText="4" value="4" />
                    <Radio labelText="5" value="5" />
                  </RadioGroup>
                  <Flex justifyContent="space-between" aria-hidden="true">
                    <Text size="caption">Strongly disagree</Text>
                    <Text size="caption">Strongly agree</Text>
                  </Flex>
                </Box>
              ) : (
                <Box marginY="m">
                  <TextInput
                    type="textarea"
                    labelText="Comment"
                    value={responses[currentQuestionIndex]}
                    maxLength={CHARACTER_LIMIT}
                    placeholder="Enter your feedback here"
                    width="100%"
                    onChange={(e) => {
                      const value = e.target.value;
                      handleResponse(currentQuestionIndex, value);
                      const currentRemaining = CHARACTER_LIMIT - value.length;
                      for (const threshold of ANNOUNCE_THRESHOLDS) {
                        if (
                          currentRemaining <= threshold &&
                          !announcedThresholdsRef.current.has(threshold)
                        ) {
                          announcedThresholdsRef.current.add(threshold);
                          setCharCountAnnouncement(
                            `${currentRemaining} characters remaining`
                          );
                        }
                      }
                    }}
                  />
                  <Text size="caption" aria-hidden="true">
                    {remainingChars} characters remaining
                  </Text>
                </Box>
              )}
              <ButtonGroup display="flex" sx={{ width: "100% !important" }}>
                <Button
                  variant="secondary"
                  isDisabled={currentQuestionIndex === 0}
                  alignItems="center"
                  display="flex"
                  flex="1 0 0"
                  onClick={() => setCurrentQuestionIndex((prev) => prev - 1)}
                >
                  <Icon name="arrow" iconRotation="rotate90" size="xsmall" />
                  Previous
                </Button>
                {isLastQuestion ? (
                  <Button variant="primary" flex="1" onClick={onSubmit}>
                    Submit
                  </Button>
                ) : (
                  <Button
                    variant="secondary"
                    alignItems="center"
                    display="flex"
                    flex="1 0 0"
                    onClick={() => setCurrentQuestionIndex((prev) => prev + 1)}
                  >
                    Next
                    <Icon name="arrow" iconRotation="rotate270" size="xsmall" />
                  </Button>
                )}
              </ButtonGroup>
            </Box>
          </>
        )}
        {isConfirmation && (
          <>
            <Button
              variant="iconOnly"
              aria-label="Close survey"
              border="none"
              color="ui.black"
              height="fit-content"
              minWidth="24px"
              width="24px"
              marginLeft="auto"
              padding="0"
              onClick={closeSurvey}
            >
              <Icon name="close" size="large" />
            </Button>
            <Box
              ref={confirmationRef}
              fontSize="desktop.body.body2"
              paddingBottom="l"
              textAlign="center"
              tabIndex={-1}
              sx={{ "&:focus": { outline: "none" } }}
            >
              <Icon
                color="ui.success.primary"
                name="actionCheckCircleFilled"
                size="large"
              />
              <Text fontWeight="medium">Thank you for your feedback.</Text>
              <Text>We look forward to improving your experience.</Text>
            </Box>
          </>
        )}
      </Form>
    </FocusLock>
  );
};

export default VRAPopupSurvey;
