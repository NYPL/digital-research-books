import type { NextApiRequest, NextApiResponse } from "next";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";
import {
  CLOUDFRONT_DOMAIN,
  generateSignedCookies,
} from "~/src/lib/aws/cloudfront";

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

  // TODO: use itemID to fetch the barcode (and title)
  // `itemsReadFetcher` does not currently return the barcode, so we may need to create a new API route that returns the necessary
  // metadata for manifest generation based on the itemId. For now, we are hardcoding the barcode for testing purposes.
  // const itemReadResult = await itemsReadFetcher(itemId as string, pageId);
  const barcode = "33333011962152";
  const pageId = req.query.pageId as string | undefined;
  const title = "Sample PDF Document";

  const readingOrder = [
    {
      // href: `https://drb-files-limited-production.s3.us-east-1.amazonaws.com/pdfs/33333011962152.pdf`,
      // href: `https://dod1h09w6yope.cloudfront.net/pdfs/33433062509165.pdf`,
      // href: `https://${CLOUDFRONT_DOMAIN}/pdfs/33433062509165.pdf?start=${pageId}`,
      href: `https://sandbox-vra-experiments-dev-public.s3.us-east-1.amazonaws.com/${barcode}.pdf?start=${pageId}`,
      type: "application/pdf",
      title,
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

  // Generate CloudFront signed cookies for secure access to the PDF file in S3
  const signedCookies = generateSignedCookies();

  res.setHeader("Content-Type", "application/webpub+json");
  res.setHeader(
    "Set-Cookie",
    Object.entries(signedCookies).map(
      ([name, value]) =>
        `${name}=${value}; Path=/; HttpOnly; Secure; SameSite=None`
    )
  );

  res.status(200).json(manifest);
}
