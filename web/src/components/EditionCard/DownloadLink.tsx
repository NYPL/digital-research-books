import {
  Box,
  Flex,
  Icon,
  useModal,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useState } from "react";
import Link from "~/src/components/Link/Link";
import { LOGIN_LINK_BASE } from "~/src/constants/links";
import { trackCtaClick } from "~/src/lib/adobe/Analytics";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import { fulfillFetcher } from "~/src/lib/api/SearchApi";
import { ItemLink } from "~/src/types/DataModel";
import { formatUrl } from "~/src/util/Util";
import { LOGIN_TO_DOWNLOAD_TEST_ID } from "~/src/constants/testIds";

const DownloadLink: React.FC<{
  authors: string[];
  downloadLink: ItemLink;
  title: string;
  isLoggedIn: boolean;
  loginCookie?: any;
}> = ({ authors, downloadLink, title, isLoggedIn, loginCookie }) => {
  const router = useRouter();
  let errorModalMessage;
  const { onOpen, Modal } = useModal();
  const [modalProps, setModalProps] = useState({
    bodyContent: null,
    closeButtonLabel: "",
    headingText: null,
  });
  if (downloadLink && downloadLink.url) {
    let linkText = "Download PDF";
    let linkUrl = formatUrl(downloadLink.url);

    const handleDownload = async (e) => {
      if (linkUrl.includes("/fulfill/")) {
        e.preventDefault();
        const errorMessage = await fulfillFetcher(linkUrl, loginCookie, router);
        if (errorMessage !== undefined) {
          errorModalMessage = (
            <span>
              We were unable to download your item. The system reports ‘
              {errorMessage}’. <br /> Please try again or{" "}
              <Link to="https://www.nypl.org/get-help/contact-us">
                contact us
              </Link>{" "}
              for assistance.
            </span>
          );
          const errorHeading = (
            <Flex alignItems="center">
              <Icon color="ui.error.primary" name="errorFilled" size="large" />
              <Box paddingLeft="xs">Download failed</Box>
            </Flex>
          );
          setModalProps({
            bodyContent: errorModalMessage,
            closeButtonLabel: "OK",
            headingText: errorHeading,
          });
          onOpen();
        }
      }
      trackCtaClick({
        cta_section: `${title}`,
        cta_text: "Download",
        destination_url: `${linkUrl}`,
      });
      trackEvent({
        "event":  "file_download",
        "click_text": linkText,
        "file_extension": downloadLink.mediaType == "application/pdf" ? "pdf" : "epub",
        "file_name": title,
        "item_title": title,
        "item_author": authors,
      });
    };

    if (downloadLink.flags.nypl_login && !isLoggedIn) {
      linkText = "Log in to download PDF";
      linkUrl = LOGIN_LINK_BASE + encodeURIComponent(window.location.href);
    }

    return (
      <Box data-testid={LOGIN_TO_DOWNLOAD_TEST_ID}>
        <Link
          to={`${linkUrl}`}
          linkType="buttonSecondary"
          onClick={(e) => handleDownload(e)}
          aria-label={`${title} Download PDF`}
        >
          <Icon
            name="download"
            align="left"
            size="small"
            decorative
            iconRotation="rotate0"
          />
          {linkText}
        </Link>
        <Modal {...modalProps}></Modal>
      </Box>
    );
  }
};

export default DownloadLink;
