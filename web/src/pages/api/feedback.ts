import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { type, ...fields } = req.body;

  if (!type || (type !== "drb" && type !== "vra")) {
    return res
      .status(400)
      .json({ error: 'Request body must include type "drb" or "vra".' });
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) {
    console.error("Missing AIRTABLE_API_KEY environment variable.");
    return res.status(500).json({ error: "Server configuration error." });
  }

  const formURL =
    type === "vra"
      ? appConfig.feedback.vraFormURL
      : appConfig.feedback.drbFormURL;

  const airtableFields: Record<string, string> = {
    Feedback: fields.feedback,
    Category: fields.category,
    Date: new Date().toLocaleString("en-US"),
    Environment: process.env.APP_ENV ?? "",
    URL: fields.url,
  };
  if (fields.email) airtableFields["Email"] = fields.email;
  if (type === "vra") {
    airtableFields["Session ID"] = fields.sessionId ?? "";
    airtableFields["Thumbs up/down"] = fields.thumbState ?? "";
  }

  try {
    const airtableRes = await fetch(formURL, {
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
