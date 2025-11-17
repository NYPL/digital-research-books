import appConfig from "~/config/appConfig";
import type { NextApiRequest, NextApiResponse } from "next";

// Disable the built-in body parser so we can stream
export const config = {
  api: {
    bodyParser: false,
  },
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (process.env.APP_ENV === "production") {
    res.status(403).json({
      error: "Research Assistant API is not enabled in this environment.",
    });
    return;
  }

  try {
    const apiEnv = process.env["APP_ENV"];
    const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];
    const apiKey = process.env["API_KEY"];

    const { chatsPath } = appConfig.api;
    const streamChatsUrl = apiUrl + chatsPath + "/stream";

    let body = "";
    await new Promise<void>((resolve) => {
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => resolve());
    });

    const { messages } = JSON.parse(body);

    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      res.status(400).json({
        error: 'Request body must contain a non-empty "messages" array.',
      });
      return;
    }

    const authorization = req.headers.authorization || null;
    const streamRes = await fetch(streamChatsUrl, {
      method: "POST", // Must be POST
      headers: {
        "Content-Type": "application/json",
        "X-API-KEY": apiKey,
        Authorization: authorization || "",
      },
      body: JSON.stringify({ messages }),
    });

    // Proxy status code and headers
    res.status(streamRes.status);
    streamRes.headers.forEach((value, key) => {
      if (
        !["transfer-encoding", "content-length", "connection"].includes(
          key.toLowerCase()
        )
      ) {
        res.setHeader(key, value);
      }
    });

    if (!streamRes.body) {
      res.status(500).json({ error: "No stream body from backend." });
      return;
    }

    // Pipe the stream to the response
    const reader = streamRes.body.getReader();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) res.write(Buffer.from(value));
    }
    res.end();
  } catch (error) {
    console.error(
      "Error in Next.js Research Assistant STREAM API route:",
      error
    );
    res.status(500).json({
      error:
        "Internal server error processing research assistant stream request.",
    });
  }
}
