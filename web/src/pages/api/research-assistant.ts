import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (process.env.APP_ENV === "production") {
    return res.status(403).json({
      error: "Research Assistant API is not enabled in this environment.",
    });
  }

  try {
    const apiEnv = process.env["APP_ENV"];
    const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];
    const apiKey = process.env["API_KEY"];

    const { chatPath } = appConfig.api;
    const chatUrl = apiUrl + chatPath;

    if (!apiUrl || !apiKey) {
      console.error(
        "Missing Python backend URL or API Key environment variables."
      );
      return res.status(500).json({ error: "Server configuration error." });
    }

    const { message, conversationType, editionId, sessionId } = req.body;

    if (!message || typeof message !== "string") {
      return res.status(400).json({
        error: 'Request body must contain a "message" string.',
      });
    }

    const authorization = req.headers.authorization || undefined;

    const payload: any = {
      message,
      conversationType,
      sessionId,
    };

    if (editionId !== undefined) payload.editionId = editionId;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-KEY": apiKey,
    };
    if (authorization) headers.Authorization = authorization;

    const chatResponse = await fetch(chatUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    if (!chatResponse.ok && chatResponse.status !== 201) {
      const errorData = await chatResponse.json();
      console.error("Error from Python Research Assistant backend:", errorData);
      return res.status(chatResponse.status).json({
        error: errorData.message || "Error from research assistant backend.",
      });
    }

    const chatResult = await chatResponse.json();
    const chatData = chatResult.data;

    return res.status(201).json({
      results: chatData.result,
      messages: chatData.messages,
      resultType: chatData.result_type,
    });
  } catch (error) {
    console.error("Error in Next.js Research Assistant API route:", error);
    return res.status(500).json({
      error: "Internal server error processing research assistant request.",
    });
  }
}
