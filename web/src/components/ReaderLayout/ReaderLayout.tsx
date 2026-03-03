import { Breadcrumbs, DSProvider } from "@nypl/design-system-react-components";
import { addTocToManifest } from "@nypl/web-reader";
import dynamic from "next/dynamic";
import { useRouter } from "next/router";
import React, { useEffect, useState } from "react";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import { defaultBreadcrumbs } from "~/src/constants/labels";
import { MediaTypes } from "~/src/constants/mediaTypes";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import ErrorComponent from "~/src/pages/_error";
import { ApiLink, LinkResult } from "~/src/types/LinkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { formatUrl, truncateStringOnWhitespace } from "~/src/util/Util";
import IFrameReader from "../IFrameReader/IFrameReader";
import Layout from "../Layout/Layout";
import Loading from "../Loading/Loading";

const WebReader = dynamic(() => import("@nypl/web-reader"), { ssr: false });

const origin =
  typeof window !== "undefined" && window.location?.origin
    ? window.location.origin
    : "";

const injectables = [
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-before.css"}`,
  },
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-default.css"}`,
  },
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-after.css"}`,
  },
  {
    type: "style",
    url: `${origin}/fonts/opendyslexic/opendyslexic.css`,
    fontFamily: "opendyslexic",
  },
];

//The NYPL wrapper that wraps the Reader pages.
const ReaderLayout: React.FC<{
  linkResult: LinkResult;
  proxyUrl: string;
  backUrl: string;
}> = (props) => {
  const link: ApiLink = props.linkResult.data;
  const url = formatUrl(link.url);
  const proxyUrl = props.proxyUrl;
  const edition = link.work.editions[0];
  const [manifestUrl, setManifestUrl] = useState(url);
  const [isLoading, setIsLoading] = useState(true);
  const [useProxyUrl, setUseProxyUrl] = useState(true);

  const isEmbed = MediaTypes.embed.includes(link.media_type);
  const isRead = MediaTypes.read.includes(link.media_type);
  const isLimitedAccess = link.flags.fulfill_limited_access;

  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const nyplIdentityCookie = cookies[NYPL_SESSION_ID];
  const router = useRouter();

  const pdfWorkerSrc = `${origin}/pdf-worker/pdf.worker.min.mjs`;

  const { page } = useResultPageContext();

  /**
   * This is a function we will use to get the resource through a given proxy url.
   * It will eventually be passed to the web reader instead of passing a proxy url directly.
   */
  const getProxiedResource = (proxyUrl?: string) => async (href: string) => {
    // Generate the resource URL using the proxy if the URI is not stored in S3
    const isResourceSelfHosted = href.includes("drb-files");
    const shouldProxyUrl = proxyUrl && !isResourceSelfHosted;
    setUseProxyUrl(shouldProxyUrl);

    const url: string = shouldProxyUrl
      ? `${proxyUrl}${encodeURIComponent(href)}`
      : href;

    const response = await fetch(url, { mode: "cors" });
    const array = new Uint8Array(await response.arrayBuffer());

    if (!response.ok) {
      throw new Error("Response not Ok for URL: " + url);
    }
    return array;
  };

  useEffect(() => {
    if (isRead) {
      /**
       * - Fetches manifest
       * - Adds the TOC to the manifest
       * - Generates a syncthetic url for the manifest to be passed to
       * web reader.
       * - Returns the synthetic url
       */
      const fetchAndModifyManifest = async (url) => {
        setIsLoading(true);
        const response = await fetch(url);
        const manifest = await response.json();
        if (
          manifest &&
          manifest.readingOrder &&
          manifest.readingOrder.length === 1 &&
          !isLimitedAccess
        ) {
          const modifiedManifest = await addTocToManifest(
            manifest,
            getProxiedResource(proxyUrl),
            pdfWorkerSrc
          );
          const syntheticUrl = URL.createObjectURL(
            new Blob([JSON.stringify(modifiedManifest)])
          );
          setManifestUrl(syntheticUrl);
        }
        setIsLoading(false);
      };

      fetchAndModifyManifest(url);

      // hides header and footer components when web reader is displayed
      if (page !== "vra") {
        document.getElementById("nypl-header").style.display = "none";
        document.getElementById("nypl-footer").style.display = "none";
      }
    }
  }, [isLimitedAccess, isRead, pdfWorkerSrc, proxyUrl, url, page]);

  if (!isEmbed && !isRead) {
    return <ErrorComponent statusCode={404} />;
  }

  return (
    <>
      {isEmbed && (
        <Layout>
          <Breadcrumbs
            variant="research"
            breadcrumbsData={[
              ...defaultBreadcrumbs,
              {
                url: `/work/${edition.work_uuid}`,
                text: truncateStringOnWhitespace(
                  edition.title,
                  MAX_TITLE_LENGTH
                ),
              },
              {
                url: `/edition/${edition.edition_id}`,
                text: EditionCardUtils.editionYearText(edition),
              },
            ]}
          />
          <IFrameReader url={link.url} />
        </Layout>
      )}
      {isRead && isLoading && <Loading />}
      {isRead && !isLoading && (
        <DSProvider>
          <WebReader
            webpubManifestUrl={manifestUrl}
            proxyUrl={!isLimitedAccess && useProxyUrl ? proxyUrl : undefined}
            pdfWorkerSrc={pdfWorkerSrc}
            injectablesFixed={injectables}
            getContent={
              isLimitedAccess
                ? EditionCardUtils.createGetContent(nyplIdentityCookie, router)
                : undefined
            }
          />
        </DSProvider>
      )}
    </>
  );
};

export default ReaderLayout;
