import type { NextApiRequest, NextApiResponse } from "next";

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "DELETE") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const cookieName = process.env.SESSION_COOKIE_NAME || "vra_session";
  const isNotDevelopment = process.env.APP_ENV !== "development";

  res.setHeader(
    "Set-Cookie",
    `${cookieName}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${
      isNotDevelopment ? "; Secure" : ""
    }`
  );

  return res.status(204).end();
}
