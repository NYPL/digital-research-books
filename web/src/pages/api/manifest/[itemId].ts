import type { NextApiRequest, NextApiResponse } from "next";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";

/* TODO: Evaluate if manifest generation should be moved to the backend */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { itemId } = req.query;
  const pageId = req.query.pageId as string | undefined;

  // TODO: Look into caching this result to remove redundant fetches
  const itemReadResult = await itemsReadFetcher(itemId as string, pageId);
  const data = itemReadResult?.data;

  if (!data) {
    return res.status(404).json({ error: "No data found" });
  }

  const protocol = req.headers["x-forwarded-proto"] || "http";
  const host = req.headers.host;
  const baseUrl = `${protocol}://${host}`;

  const pageIds: string[] = [
    ...(data.previousPages || []),
    pageId!,
    ...(data.nextPages || []),
  ].filter(Boolean);

  const readingOrder = pageIds.map((pid) => ({
    href: `${baseUrl}/api/pdf/${itemId}/${pid}`,
    type: "application/pdf",
    title: `PDF Page ${pid}`,
  }));

  const manifest = {
    "@context": "https://readium.org/webpub-manifest/context.jsonld",
    metadata: {
      title: "PDF Document",
      conformsTo: "http://librarysimplified.org/terms/profiles/pdf",
    },
    readingOrder: readingOrder,
  };

  res.setHeader("Content-Type", "application/webpub+json");
  res.status(200).json(manifest);
}
