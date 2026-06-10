import Head from "next/head";
import React from "react";
import Edition from "~/src/components/EditionDetail/Edition";
import Layout from "~/src/components/Layout/Layout";
import { MAX_PAGE_TITLE_LENGTH } from "~/src/constants/editioncard";
import { documentTitles } from "~/src/constants/labels";
import { editionFetcher } from "~/src/lib/api/SearchApi";
import { EditionQuery, EditionResult } from "~/src/types/EditionQuery";
import { getBackToSearchUrl } from "~/src/util/LinkUtils";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import Error from "../_error";

export async function getServerSideProps(context: any) {
  const editionQuery: EditionQuery = {
    editionIdentifier: context.query.editionId,
    showAll: context.query.showAll,
  };

  const backUrl = getBackToSearchUrl(
    context.req.headers.referer,
    context.req.headers.host
  );

  const editionResult: EditionResult = await editionFetcher(editionQuery);
  return {
    props: { editionResult: editionResult, backUrl: backUrl },
  };
}

const EditionResults: React.FC<any> = (props) => {
  const [displayEditionResult, setDisplayEditionResult] = React.useState(
    props.editionResult
  );

  React.useEffect(() => {
    setDisplayEditionResult(
      isBlinkClient()
        ? normalizeCombiningHalfMarksDeep(props.editionResult)
        : props.editionResult
    );
  }, [props.editionResult]);

  if (displayEditionResult.status !== 200) {
    return <Error statusCode={displayEditionResult.status} />;
  }

  return (
    <Layout>
      <Head>
        <title>
          {`${truncateStringOnWhitespace(
            displayEditionResult.data.title,
            MAX_PAGE_TITLE_LENGTH
          )} | ${documentTitles.editionItem}`}
        </title>
      </Head>
      <Edition editionResult={displayEditionResult} backUrl={props.backUrl} />
    </Layout>
  );
};

export default EditionResults;
