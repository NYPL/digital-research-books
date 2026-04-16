import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";
import { DRBFeedback, VRAFeedback } from "~/src/types/Feedback";

type FeedbackFields = DRBFeedback | VRAFeedback;
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
      fldmZTqpb3H9n1D8I: fields.feedback,
      fldZYABlI2qvpZ77t: fields.category,
      fldECb5srxCM9xPpv: new Date().toLocaleString("en-US"),
      fldJ70WE84MvEpPmh: process.env.APP_ENV ?? "",
      fldCHANfhWHyMc9u7: fields.url,
      fldvQ4YxqeiGRImB6: fields.sessionId,
      fldrD25IvZZXQ8zrC: fields.thumbState,
      ...(fields.email && { fld8iTQqzy2Xe69jH: fields.email }),
    }),
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
