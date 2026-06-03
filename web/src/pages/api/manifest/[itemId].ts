import type { NextApiRequest, NextApiResponse } from "next";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";

const CLOUDFRONT_DOMAIN = "d2dqbod0obw22i.cloudfront.net";

/* TODO: Evaluate if manifest generation should be moved to the backend */

/**
 * API Route handler for:
 * - Returning a Web Publication Manifest for a given item.
 * - Responding with a CloudFront signed cookie for secure access to the PDF file in S3.
 * - Frontend is expected to use that manifest to load the PDF in the Web Reader component,
 *   which will use the signed cookie for authentication when fetching the PDF from S3.
 */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { itemId } = req.query;

  const pageId = req.query.pageId as string | undefined;
  const itemReadResult = await itemsReadFetcher(itemId as string, pageId);
  const { barcode } = itemReadResult.data;

  const readingOrder = [
    {
      href: `https://${CLOUDFRONT_DOMAIN}/pdfs/${barcode}.pdf?start=${pageId}`,
      type: "application/pdf",
      title: `Page ${pageId}`,
    },
  ];

  const manifest = {
    "@context": "https://readium.org/webpub-manifest/context.jsonld",
    metadata: {
      title: "PDF Document",
      conformsTo: "http://librarysimplified.org/terms/profiles/pdf",
    },
    readingOrder: readingOrder,
  };

  // TODO: generate signed cookies here and append to the response header
  //
  // const signedCookies = generateSignedCookies();
  // res.setHeader(
  //   "Set-Cookie",
  //   Object.entries(signedCookies).map(([name, value]) => `${name}=${value}; Path=/; HttpOnly; Secure; SameSite=None`)
  // );

  res.setHeader("Content-Type", "application/webpub+json");
  res.status(200).json(manifest);
}
