import dynamic from "next/dynamic";
import React from "react";
import { proxyUrlConstructor, readFetcher } from "~/src/lib/api/SearchApi";
import { log } from "~/src/lib/newrelic/NewRelic";
import { LinkResult } from "~/src/types/LinkQuery";
import { getBackUrl } from "~/src/util/LinkUtils";
import Error from "../_error";

const ReaderLayout = dynamic(
  () => import("~/src/components/ReaderLayout/ReaderLayout"),
  { ssr: false }
);

export async function getServerSideProps(context: any) {
  try {
    const linkResult: LinkResult = await readFetcher(context.query.linkId);
    const proxyUrl: string = proxyUrlConstructor();
    const backUrl = getBackUrl(
      context.req.headers.referer,
      context.req.headers.host
    );
    return {
      props: {
        linkResult: linkResult,
        proxyUrl: proxyUrl,
        backUrl: backUrl,
      },
    };
  } catch (e) {
    log(e, "Failed to fetch link");
    return {
      notFound: true,
    };
  }
}

const WebReaderPage: React.FC<any> = (props) => {
  if (props.linkResult.status !== 200) {
    return <Error statusCode={props.linkResult.status} />;
  }

  return (
    <ReaderLayout
      linkResult={props.linkResult}
      proxyUrl={props.proxyUrl}
      backUrl={props.backUrl}
    />
  );
};
export default WebReaderPage;
