import Head from "next/head";
import { useRouter } from "next/router";
import React from "react";
import ItemDetail from "~/src/components/ItemDetail/ItemDetail";
import Layout from "~/src/components/Layout/Layout";
import VRAFeedback from "~/src/components/VRAFeedback/VRAFeedback";
import VRALayout from "~/src/components/VRALayout/VRALayout";
import { MAX_PAGE_TITLE_LENGTH } from "~/src/constants/editioncard";
import { documentTitles } from "~/src/constants/labels";
import { workFetcher } from "~/src/lib/api/SearchApi";
import { WorkQuery, WorkResult } from "~/src/types/WorkQuery";
import { getBackToVraUrl } from "~/src/util/LinkUtils";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import Error from "../_error";

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
  const router = useRouter();
  const [displayWorkResult, setDisplayWorkResult] = React.useState(
    props.workResult
  );

  React.useEffect(() => {
    setDisplayWorkResult(
      isBlinkClient()
        ? normalizeCombiningHalfMarksDeep(props.workResult)
        : props.workResult
    );
  }, [props.workResult]);

  if (displayWorkResult.status !== 200) {
    return <Error statusCode={displayWorkResult.status} />;
  }

  const truncatedTitle = truncateStringOnWhitespace(
    displayWorkResult.data.title,
    MAX_PAGE_TITLE_LENGTH
  );

  return (
    <Layout feedback={<VRAFeedback />}>
      <Head>
        <title>{`${truncatedTitle} | ${documentTitles.workItem}`}</title>
      </Head>
      <VRALayout
        activePage="item"
        breadcrumbsData={[
          {
            url: `/item/${displayWorkResult.data.uuid}`,
            text: displayWorkResult.data.title,
          },
        ]}
      >
        <ItemDetail workResult={displayWorkResult} backUrl={props.backUrl} />
      </VRALayout>
    </Layout>
  );
};

export default ItemPage;
