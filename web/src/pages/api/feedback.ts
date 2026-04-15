import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";
import {
  DRBFeedback,
  PopupSurveyFeedback,
  VRAFeedback,
} from "~/src/types/Feedback";

type FeedbackFields = DRBFeedback | VRAFeedback | PopupSurveyFeedback;
type AirtableFields = Record<string, string>;

interface FeedbackHandler {
  formURL: string;
  mapFields: (fields: FeedbackFields) => AirtableFields;
}

const feedbackHandlers: Record<string, FeedbackHandler> = {
  drb: {
    formURL: appConfig.feedback.drbFormURL,
    mapFields: (fields: DRBFeedback): AirtableFields => ({
      Feedback: fields.feedback,
      Category: fields.category,
      Date: new Date().toLocaleString("en-US"),
      Environment: process.env.APP_ENV ?? "",
      URL: fields.url,
      ...(fields.email && { Email: String(fields.email) }),
    }),
  },

  vra: {
    formURL: appConfig.feedback.vraFormURL,
    mapFields: (fields: VRAFeedback): AirtableFields => ({
      Feedback: fields.feedback,
      Category: fields.category,
      Date: new Date().toLocaleString("en-US"),
      Environment: process.env.APP_ENV ?? "",
      URL: fields.url,
      "Session ID": fields.sessionId,
      "Thumbs up/down": fields.thumbState,
      ...(fields.email && { Email: fields.email }),
    }),
  },

  vraPopup: {
    formURL: appConfig.feedback.vraPopupUrl,
    mapFields: (fields: PopupSurveyFeedback): AirtableFields => {
      const responses = fields.responses;
      return {
        fldFYcj0dD3DYkOUF: responses[0],
        fldp7kwoR2ZLJsX6M: responses[1],
        fldTAM3mKfUGChBz7: responses[2],
        fldFDnSBjZbPxS1NT: responses[3],
        fldgod6a2P1BQUrYZ: responses[4],
        fldj3N2Zvmqano680: fields.sessionId,
        fld8tezDVVVc03SOm: new Date().toLocaleString("en-US"),
        fldBlsYdy9t0iUqqG: fields.url,
        fldTTU0PHBcY4y2Uc: process.env.APP_ENV ?? "",
      };
    },
  },
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { type, ...fields } = req.body;

  if (!type || !feedbackHandlers[type]) {
    return res.status(400).json({
      error: `Invalid feedback type. Must be one of: ${Object.keys(
        feedbackHandlers
      ).join(", ")}`,
    });
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) {
    console.error("Missing AIRTABLE_API_KEY environment variable.");
    return res.status(500).json({ error: "Server configuration error." });
  }

  const handler = feedbackHandlers[type];
  const airtableFields = handler.mapFields(fields);

  try {
    const airtableRes = await fetch(handler.formURL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: airtableFields }),
    });

    if (!airtableRes.ok) {
      const errorData = await airtableRes.json();
      console.error("Airtable error:", errorData);
      return res
        .status(airtableRes.status)
        .json({ error: "Failed to submit feedback." });
    }

    return res.status(201).end();
  } catch (error) {
    console.error("Error submitting feedback:", error);
    return res.status(500).json({ error: "Internal server error." });
  }
}
