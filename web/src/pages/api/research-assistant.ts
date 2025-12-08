import appConfig from "~/config/appConfig";
import type { NextApiRequest, NextApiResponse } from "next";

export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    if (process.env.APP_ENV === "production") {
        return res
            .status(403)
            .json({
                error: "Research Assistant API is not enabled in this environment.",
            });
    }

    try {
        const apiEnv = process.env["APP_ENV"];
        const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];
        const apiKey = process.env["API_KEY"];

        const { chatsPath } = appConfig.api;
        const chatsUrl = apiUrl + chatsPath;

        if (!apiUrl || !apiKey) {
            console.error(
                "Missing Python backend URL or API Key environment variables."
            );
            return res.status(500).json({ error: "Server configuration error." });
        }

        const { messages, initialMessageType } = req.body;

        if (!messages || !Array.isArray(messages)) {
            return res
                .status(400)
                .json({
                    error: 'Request body must contain a "messages" array.',
                });
        }
        
        const authorization = req.headers.authorization || null;

        const chatsResponse = await fetch(chatsUrl, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
                "X-API-KEY": apiKey,
                "Authorization": authorization
            },
            body: JSON.stringify({ messages, initialMessageType }),
        });

        if (!chatsResponse.ok && chatsResponse.status !== 201) {
            const errorData = await chatsResponse.json();
            console.error("Error from Python Research Assistant backend:", errorData);
            return res
                .status(chatsResponse.status)
                .json({
                    error: errorData.message || "Error from research assistant backend.",
                });
        }

        const chatsResult = await chatsResponse.json();
        const chatsData = chatsResult.data;

        return res.status(201).json({
            answer: chatsData.answer,
            results: chatsData.results,
            messages: chatsData.messages,
        });
    } catch (error) {
        console.error("Error in Next.js Research Assistant API route:", error);
        return res
            .status(500)
            .json({
                error: "Internal server error processing research assistant request.",
            });
    }
}
