import React from "react";
import KeywordSearch from "~/src/components/KeywordSearch/KeywordSearch";
import Layout from "~/src/components/NewLayout/Layout";
import VRALayout from "~/src/components/VRALayout/VRALayout";
import { toSearchQuery } from "~/src/util/apiConversion";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import { searchResultsFetcher } from "../../lib/api/SearchApi";
import { ApiSearchQuery } from "../../types/SearchQuery";
import Error from "../_error";

export async function getServerSideProps(context: any) {
  const isResearchAssistantEnabled = process.env.APP_ENV !== "production";
  if (!isResearchAssistantEnabled) {
    return {
      notFound: true,
    };
  }

  // Get Query from location
  const searchQuery: ApiSearchQuery = context.query;
  const searchResults = await searchResultsFetcher(searchQuery);
  const convertedQuery = toSearchQuery(searchQuery);
  return {
    props: {
      searchQuery: convertedQuery,
      searchResults: searchResults,
    },
  };
}

const SearchResults: React.FC<any> = (props) => {
  const [displaySearchResults, setDisplaySearchResults] = React.useState(
    props.searchResults
  );

  React.useEffect(() => {
    setDisplaySearchResults(
      isBlinkClient()
        ? normalizeCombiningHalfMarksDeep(props.searchResults)
        : props.searchResults
    );
  }, [props.searchResults]);

  if (displaySearchResults.status !== 200) {
    return <Error statusCode={displaySearchResults.status} />;
  }

  return (
    <Layout>
      <VRALayout
        activePage="keyword"
        breadcrumbsData={[
          {
            url: `/keyword-search`,
            text: "Keyword search",
          },
        ]}
      >
        <KeywordSearch
          searchQuery={props.searchQuery}
          searchResults={displaySearchResults}
        />
      </VRALayout>
    </Layout>
  );
};

export default SearchResults;
