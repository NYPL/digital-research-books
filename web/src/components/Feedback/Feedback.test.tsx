import React from "react";
import { render } from "../../__tests__/testUtils/render";
import { screen } from "@testing-library/react";
import Feedback from "./Feedback";
import userEvent from "@testing-library/user-event";

describe("Feedback", () => {
  beforeEach(() => {
    render(<Feedback location={"/testing?testQuery"} />);
  });

  describe("Form Submission", () => {
    beforeEach(async () => {
      const feedbackButton = screen.getByRole("button", {
        name: "Help and Feedback",
      });
      await userEvent.click(feedbackButton);
    });

    const fakeFetch = jest.fn();
    window.fetch = fakeFetch;
    test("should invoke method when success and feedback set", async () => {
      const feedback = screen.getByRole("textbox", {
        name: "Comment (required)",
      });
      const commentRadio = screen.getByLabelText("Comment");
      await userEvent.type(feedback, "test value");
      await userEvent.click(commentRadio);
      await userEvent.click(screen.getByRole("button", { name: "Submit" }));
      expect(fetch).toHaveBeenCalledTimes(1);
    });
  });
});
