import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";

/**
 * Streaming proxy for the research assistant endpoint.
 * Forwards streaming NDJSON responses directly to the client.
 *
 * Request body: { message: string, conversationType: string, editionId?: number }
 *
 * Response: NDJSON stream with events like:
 *   {"type": "search_started", "context": "..."}
 *   {"type": "search_completed", "status": "..."}
 *   {"type": "final_response", "messages": [...], "result_type": "...", "result": {...}}
 */
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

    const { message, conversationType, editionId } = req.body;

    if (!message || typeof message !== "string") {
      return res.status(400).json({
        error: 'Request body must contain a "message" string.',
      });
    }

    const authorization = req.headers.authorization || undefined;
    const cookieHeader = req.headers.cookie || undefined;

    const payload: any = {
      message,
      conversationType,
    };

    if (editionId !== undefined) payload.editionId = editionId;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-KEY": apiKey,
      Accept: "application/x-ndjson",
    };
    if (authorization) headers.Authorization = authorization;
    if (cookieHeader) headers["cookie"] = cookieHeader;

    const chatResponse = await fetch(chatUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const setCookie = chatResponse.headers.get("set-cookie");
    if (setCookie) {
      res.setHeader("Set-Cookie", setCookie);
    }

    if (!chatResponse.ok) {
      const errorData = await chatResponse.json();
      console.error("Error from Python Research Assistant backend:", errorData);
      return res.status(chatResponse.status).json({
        error: errorData.message || "Error from research assistant backend.",
      });
    }

    res.setHeader("Content-Type", "application/x-ndjson");
    res.setHeader("Transfer-Encoding", "chunked");
    res.setHeader("Cache-Control", "no-cache, no-transform");

    const reader = chatResponse.body?.getReader();
    if (!reader) {
      return res.status(500).json({
        error: "Failed to read response stream from backend.",
      });
    }

    const decoder = new TextDecoder();
    let buffer = "";

    try {
      let doneReading = false;
      while (!doneReading) {
        const { done, value } = await reader.read();
        doneReading = done;

        if (doneReading) {
          if (buffer.trim()) {
            res.write(buffer + "\n");
          }
          break;
        }

        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split("\n");

        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.trim()) {
            res.write(line + "\n");
          }
        }
      }
    } catch (streamError) {
      console.error("Error streaming response from backend:", streamError);
      res.write(
        JSON.stringify({
          type: "error",
          message: "Stream interrupted or backend error occurred.",
        }) + "\n"
      );
    } finally {
      res.end();
    }
  } catch (error) {
    console.error("Error in streaming research assistant route:", error);
    return res.status(500).json({
      error: "Internal server error processing streaming request.",
    });
  }
}
