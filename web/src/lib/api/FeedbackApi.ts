import {
  DRBFeedback,
  PopupSurveyResponse,
  VRAFeedback,
} from "~/src/types/Feedback";
import { log } from "../newrelic/NewRelic";

export const submitDRBFeedback = async (feedback: DRBFeedback) => {
  try {
    return await fetch("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "drb", ...feedback }),
    });
  } catch (error) {
    log(error, "Failed to submit feedback");
    throw new Error(`Failed to submit feedback: ${error.message}`);
  }
};

export const submitVRAFeedback = async (feedback: VRAFeedback) => {
  try {
    return await fetch("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "vra", ...feedback }),
    });
  } catch (error) {
    log(error, "Failed to submit feedback");
    throw new Error(`Failed to submit feedback: ${error.message}`);
  }
};

export const submitVRAPopupSurvey = async (feedback: PopupSurveyResponse) => {
  try {
    return await fetch("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "vraPopup", ...feedback }),
    });
  } catch (error) {
    log(error, "Failed to submit feedback");
    throw new Error(`Failed to submit feedback: ${error.message}`);
  }
};
