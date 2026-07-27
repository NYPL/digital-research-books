import type { NextApiRequest, NextApiResponse } from "next";
import appConfig from "~/config/appConfig";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  try {
    const apiEnv = process.env["APP_ENV"];
    const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];
    const apiKey = process.env["API_KEY"];

    if (!apiUrl || !apiKey) {
      return res
        .status(500)
        .json({ error: "Result reason service not configured." });
    }

    const { call_id, edition_id } = req.body;

    if (!call_id || !edition_id) {
      return res.status(400).json({ error: "Missing call_id and edition_id." });
    }

    const cookieHeader = req.headers.cookie || undefined;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-KEY": apiKey,
    };
    if (cookieHeader) headers["cookie"] = cookieHeader;

    const response = await fetch(`${apiUrl}/result-reason`, {
      method: "POST",
      headers,
      body: JSON.stringify({ call_id, edition_id }),
    });

    const setCookie = response.headers.get("set-cookie");
    if (setCookie) res.setHeader("Set-Cookie", setCookie);

    const result = await response.json();

    if (!response.ok) {
      return res.status(response.status).json({
        error: result.data?.message || "Result reason request failed.",
      });
    }

    return res.status(200).json(result.data);
  } catch (error) {
    console.error("Result reason API error: ", error);
    return res.status(500).json({ error: "Result reason error." });
  }
}
