import type { NextApiRequest, NextApiResponse } from "next";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { itemId, pageId } = req.query;

  const itemReadResult = await itemsReadFetcher(itemId as string, pageId as string);

  if (!itemReadResult || !itemReadResult.data || !itemReadResult.data.pageData) {
    return res.status(404).send("No PDF data found");
  }

  const pdfBuffer = Buffer.from(itemReadResult.data.pageData, "base64");
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", "inline; filename=document.pdf");
  return res.status(200).send(pdfBuffer);
}