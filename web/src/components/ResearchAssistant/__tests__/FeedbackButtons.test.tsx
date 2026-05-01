import { fireEvent, render, screen } from "@testing-library/react";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import FeedbackButtons from "../FeedbackButtons";

const createFeedbackContextValue = () => {
  const onOpen = jest.fn();
  const onClose = jest.fn();
  const setIsError = jest.fn();
  const setNotificationText = jest.fn();
  const setStatusCode = jest.fn();
  const setDescriptionText = jest.fn();
  const setThumbValue = jest.fn();
  const setSessionId = jest.fn();

  return {
    contextValue: {
      onOpen,
      onClose,
      FeedbackBox: (() => null) as any,
      isOpen: false,
      isError: null,
      setIsError,
      notificationText: null,
      setNotificationText,
      statusCode: null,
      setStatusCode,
      descriptionText: "",
      setDescriptionText,
      thumbValue: null,
      setThumbValue,
      sessionId: "",
      setSessionId,
    },
    onOpen,
    setDescriptionText,
    setThumbValue,
  };
};

describe("FeedbackButtons", () => {
  test("sets thumbs up and opens feedback on first selection", () => {
    const {
      contextValue,
      onOpen,
      setDescriptionText,
      setThumbValue,
    } = createFeedbackContextValue();

    render(
      <FeedbackContext.Provider value={contextValue as any}>
        <FeedbackButtons />
      </FeedbackContext.Provider>
    );

    const [thumbsUpButton, thumbsDownButton] = screen.getAllByRole("button");
    fireEvent.click(thumbsUpButton);

    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(setDescriptionText).toHaveBeenCalledTimes(1);
    expect(setThumbValue).toHaveBeenCalledWith("up");
    expect(thumbsUpButton).toHaveAttribute("aria-pressed", "true");
    expect(thumbsDownButton).toBeDisabled();
  });

  test("toggles the selected thumb back to default", () => {
    const {
      contextValue,
      onOpen,
      setThumbValue,
    } = createFeedbackContextValue();

    render(
      <FeedbackContext.Provider value={contextValue as any}>
        <FeedbackButtons />
      </FeedbackContext.Provider>
    );

    const [thumbsUpButton, thumbsDownButton] = screen.getAllByRole("button");

    fireEvent.click(thumbsUpButton);
    fireEvent.click(thumbsUpButton);

    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(setThumbValue).toHaveBeenNthCalledWith(1, "up");
    expect(setThumbValue).toHaveBeenNthCalledWith(2, null);
    expect(thumbsUpButton).toHaveAttribute("aria-pressed", "false");
    expect(thumbsDownButton).toBeEnabled();
  });

  test("keeps state isolated per component instance", () => {
    const { contextValue, setThumbValue } = createFeedbackContextValue();

    render(
      <FeedbackContext.Provider value={contextValue as any}>
        <FeedbackButtons />
        <FeedbackButtons />
      </FeedbackContext.Provider>
    );

    const [firstUp, firstDown, secondUp, secondDown] = screen.getAllByRole(
      "button"
    );

    fireEvent.click(firstUp);

    expect(setThumbValue).toHaveBeenCalledWith("up");
    expect(firstUp).toHaveAttribute("aria-pressed", "true");
    expect(firstDown).toBeDisabled();
    expect(secondUp).toHaveAttribute("aria-pressed", "false");
    expect(secondDown).toHaveAttribute("aria-pressed", "false");
    expect(secondUp).toBeEnabled();
    expect(secondDown).toBeEnabled();
  });
});
