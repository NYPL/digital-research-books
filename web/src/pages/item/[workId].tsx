import Head from "next/head";
import React from "react";
import Layout from "~/src/components/Layout/Layout";
import { MAX_PAGE_TITLE_LENGTH } from "~/src/constants/editioncard";
import { documentTitles } from "~/src/constants/labels";
import { workFetcher } from "~/src/lib/api/SearchApi";
import { WorkQuery, WorkResult } from "~/src/types/WorkQuery";
import { getBackToVraUrl } from "~/src/util/LinkUtils";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import Error from "../_error";
import ItemDetail from "~/src/components/ItemDetail/ItemDetail";
import { ResearchAssistantProvider } from "~/src/context/ResearchAssistantContext";
import { ResultPageProvider } from "~/src/context/ResultPageContext";

export async function getServerSideProps(context: any) {
  const isResearchAssistantEnabled = process.env.APP_ENV !== "production";
  if (!isResearchAssistantEnabled) {
    return {
      notFound: true,
    };
  }
  
  const workQuery: WorkQuery = {
    identifier: context.query.workId,
    showAll: context.query.showAll,
  };

  const backUrl = getBackToVraUrl(
    context.req.headers.referer,
    context.req.headers.host
  );

  const workResult: WorkResult = await workFetcher(workQuery);
  return {
    props: { workResult: workResult, backUrl: backUrl },
  };
}

const ItemPage: React.FC<any> = (props) => {
  if (props.workResult.status !== 200) {
    return <Error statusCode={props.workResult.status} />;
  }

  return (
    <Layout>
      <Head>
        <title>
          {`${truncateStringOnWhitespace(
            props.workResult.data.title,
            MAX_PAGE_TITLE_LENGTH
          )} | ${documentTitles.workItem}`}
        </title>
      </Head>
      <ResearchAssistantProvider>
        <ResultPageProvider value={{
          page: props.backUrl && props.backUrl.includes("research-assistant") ? "vra" : "keyword",
          onPreview: () => {},
          onReadOnline: () => {}
        }}>
          <ItemDetail workResult={props.workResult} backUrl={props.backUrl} />
        </ResultPageProvider>
      </ResearchAssistantProvider>
    </Layout>
  );
};

export default ItemPage;
