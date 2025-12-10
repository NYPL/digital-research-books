import React from "react";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { Agent, ApiItem } from "~/src/types/DataModel";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import DownloadLink from "./DownloadLink";
import EddLink from "./EddLink";
import ReadOnlineLink from "./ReadOnlineLink";
import PreviewLink from "../EditionCard/PreviewLink";

interface CtasProps {
    authors: Agent[];
    item: ApiItem | undefined;
    title: string;
    workId?: string;
    editionId?: number;
}

const Ctas: React.FC<CtasProps> = ({
    authors,
    item,
    title,
    workId,
    editionId,
}) => {
    // cookies defaults to be undefined if not fonud
    const [cookies] = useCookies([NYPL_SESSION_ID]);
    const loginCookie = cookies[NYPL_SESSION_ID];
    const isLoggedIn = !!loginCookie;

    const readOnlineLink = EditionCardUtils.getReadOnlineLink(item);
    const downloadLink = EditionCardUtils.selectDownloadLink(item);

    const authorNames = authors ? authors.map((author) => author.name) : [];

    if (readOnlineLink || downloadLink) {
        return (
            <>
                {readOnlineLink &&
                    (readOnlineLink.mediaType !== "application/ocr" ? (
                        <ReadOnlineLink
                            authors={authorNames}
                            isLoggedIn={isLoggedIn}
                            readOnlineLink={readOnlineLink}
                            title={title}
                        />
                    ) : (
                        <PreviewLink
                            previewLink={readOnlineLink}
                            workId={workId}
                            editionId={editionId}
                        />
                    ))}
                {downloadLink && (
                    <DownloadLink
                        authors={authorNames}
                        downloadLink={downloadLink}
                        title={title}
                        isLoggedIn={isLoggedIn}
                        loginCookie={loginCookie}
                    />
                )}
            </>
        );
    }

    const eddLink =
        item && item.links ? item.links.find((link) => link.flags.edd) : undefined;

    // Offer EDD if available
    if (eddLink !== undefined) {
        return <EddLink eddLink={eddLink} isLoggedIn={isLoggedIn} title={title} />;
    }

    return <>Not yet available</>;
};

export default Ctas;
