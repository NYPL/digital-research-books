import Head from "next/head";
import React from "react";
import Layout from "~/src/components/Layout/Layout";
import WorkDetail from "~/src/components/Work/Work";
import { MAX_PAGE_TITLE_LENGTH } from "~/src/constants/editioncard";
import { documentTitles } from "~/src/constants/labels";
import { workFetcher } from "~/src/lib/api/SearchApi";
import { WorkQuery, WorkResult } from "~/src/types/WorkQuery";
import { getBackToSearchUrl } from "~/src/util/LinkUtils";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import Error from "../_error";

export async function getServerSideProps(context: any) {
  //TODO: Default query
  const workQuery: WorkQuery = {
    identifier: context.query.workId,
    showAll: context.query.showAll,
  };

  const backUrl = getBackToSearchUrl(
    context.req.headers.referer,
    context.req.headers.host
  );

  const workResult: WorkResult = await workFetcher(workQuery);
  return {
    props: { workResult: workResult, backUrl: backUrl },
  };
}

const WorkResults: React.FC<any> = (props) => {
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

  return (
    <Layout>
      <Head>
        <title>
          {`${truncateStringOnWhitespace(
            displayWorkResult.data.title,
            MAX_PAGE_TITLE_LENGTH
          )} | ${documentTitles.workItem}`}
        </title>
      </Head>
      <WorkDetail workResult={displayWorkResult} backUrl={props.backUrl} />
    </Layout>
  );
};

export default WorkResults;
