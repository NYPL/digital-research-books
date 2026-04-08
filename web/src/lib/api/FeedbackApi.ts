import appConfig from "~/config/appConfig";
import { DRBFeedback, VRAFeedback } from "~/src/types/Feedback";
import { log } from "../newrelic/NewRelic";

// TODO: disable feedback in development

export const submitDRBFeedback = async (feedback: DRBFeedback) => {
  try {
    return await fetch(appConfig.feedback.drbFormURL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_AIRTABLE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        fields: {
          Feedback: feedback.feedback,
          Category: feedback.category,
          Date: new Date().toLocaleDateString("en-US"),
          Environment: process.env.APP_ENV,
          URL: feedback.url,
        },
      }),
    });
  } catch (error) {
    log(error, "Failed to submit feedback");
    throw new Error(`Failed to submit feedback: ${error.message}`);
  }
};

export const submitVRAFeedback = async (feedback: VRAFeedback) => {
  try {
    return await fetch(appConfig.feedback.vraFormURL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_AIRTABLE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        fields: {
          Feedback: feedback.feedback,
          Category: feedback.category,
          Date: new Date().toLocaleDateString("en-US"),
          Environment: process.env.APP_ENV,
          URL: feedback.url,
          "Session ID": feedback.sessionId,
          "Thumbs up/down": feedback.thumbState,
        },
      }),
    });
  } catch (error) {
    log(error, "Failed to submit feedback");
    throw new Error(`Failed to submit feedback: ${error.message}`);
  }
};
