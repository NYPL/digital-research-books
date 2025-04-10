import React, { useEffect, useState } from "react";
import { Breadcrumbs, Icon } from "@nypl/design-system-react-components";
import { defaultBreadcrumbs } from "~/src/constants/labels";
import { ApiLink, LinkResult } from "~/src/types/LinkQuery";
import IFrameReader from "../IFrameReader/IFrameReader";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import Layout from "../Layout/Layout";
import { formatUrl, truncateStringOnWhitespace } from "~/src/util/Util";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import dynamic from "next/dynamic";
import { MediaTypes } from "~/src/constants/mediaTypes";
import ReaderLogoSvg from "../Svgs/ReaderLogoSvg";
import Link from "../Link/Link";
import { addTocToManifest } from "@nypl/web-reader";
import Loading from "../Loading/Loading";
import { trackCtaClick } from "~/src/lib/adobe/Analytics";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { useCookies } from "react-cookie";
import { useRouter } from "next/router";

const WebReader = dynamic(() => import("@nypl/web-reader"), { ssr: false });
// This is how we can import a css file as a url. It's complex, but necessary
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import readiumBefore from "!file-loader!extract-loader!css-loader!@nypl/web-reader/dist/injectable-html-styles/ReadiumCSS-before.css";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import readiumDefault from "!file-loader!extract-loader!css-loader!@nypl/web-reader/dist/injectable-html-styles/ReadiumCSS-default.css";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import readiumAfter from "!file-loader!extract-loader!css-loader!@nypl/web-reader/dist/injectable-html-styles/ReadiumCSS-after.css";
import ErrorComponent from "~/src/pages/_error";

const origin =
  typeof window !== "undefined" && window.location?.origin
    ? window.location.origin
    : "";

const injectables = [
  {
    type: "style",
    url: `${origin}${readiumBefore}`,
  },
  {
    type: "style",
    url: `${origin}${readiumDefault}`,
  },
  {
    type: "style",
    url: `${origin}${readiumAfter}`,
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
  const [useProxyUrl, setUseProxyUrl] = useState(false);

  const isEmbed = MediaTypes.embed.includes(link.media_type);
  const isRead = MediaTypes.read.includes(link.media_type);
  const isLimitedAccess = link.flags.fulfill_limited_access;

  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const nyplIdentityCookie = cookies[NYPL_SESSION_ID];
  const router = useRouter();

  const pdfWorkerSrc = `${origin}/pdf-worker/pdf.worker.min.js`;

  /**
   * This is a function we will use to get the resource through a given proxy url.
   * It will eventually be passed to the web reader instead of passing a proxy url directly.
   */
  const getProxiedResource = (proxyUrl?: string) => async (href: string) => {
    // Generate the resource URL using the proxy if the URI is not stored in S3
    const shouldProxyUrl = proxyUrl && !href.includes('drb-files');
    setUseProxyUrl(shouldProxyUrl);

    const url: string = shouldProxyUrl 
      ? `${proxyUrl}${encodeURIComponent(href)}`
      : href;

    console.log(`Getting resource from ${url}`)
    const response = await fetch(url, { mode: "cors" });
    const array = new Uint8Array(await response.arrayBuffer());

    if (!response.ok) {
      throw new Error("Response not Ok for URL: " + url);
    }
    return array;
  };

  useEffect(() => {
    trackCtaClick({
      cta_section: `${link.work.title}`,
      cta_subsection: `${edition.edition_id}`,
      cta_text: "Read Online",
      destination_url: `${link.url}`,
    });
  }, [edition.edition_id, link]);

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
      document.getElementById("nypl-header").style.display = "none";
      document.getElementById("nypl-footer").style.display = "none";
    }
  }, [isLimitedAccess, isRead, pdfWorkerSrc, proxyUrl, url]);

  const BackButton = () => {
    return (
      //Apends design system classname to use Design System Link.
      <Link to={props.backUrl} type="action" className="nypl-ds logo-link">
        <Icon decorative className="logo-link__icon">
          <ReaderLogoSvg />
        </Icon>
        <span className="logo-link__label">Back to Digital Research Books</span>
      </Link>
    );
  };

  if (!isEmbed && !isRead) {
    return <ErrorComponent statusCode={404} />;
  }

  return (
    <>
      {isEmbed && (
        <Layout>
          <Breadcrumbs
            breadcrumbsType="research"
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
        <WebReader
          webpubManifestUrl={manifestUrl}
          proxyUrl={useProxyUrl ? proxyUrl : undefined}
          pdfWorkerSrc={pdfWorkerSrc}
          headerLeft={<BackButton />}
          injectablesFixed={injectables}
          getContent={
            isLimitedAccess
              ? EditionCardUtils.createGetContent(nyplIdentityCookie, router)
              : undefined
          }
        />
      )}
    </>
  );
};

export default ReaderLayout;
