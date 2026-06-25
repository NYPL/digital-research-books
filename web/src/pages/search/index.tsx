import React from "react";
import Layout from "~/src/components/Layout/Layout";
import { toSearchQuery } from "~/src/util/apiConversion";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import Search from "../../components/Search/Search";
import { searchResultsFetcher } from "../../lib/api/SearchApi";
import { ApiSearchQuery } from "../../types/SearchQuery";
import Error from "../_error";

export async function getServerSideProps(context: any) {
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
      <Search
        searchQuery={props.searchQuery}
        searchResults={displaySearchResults}
      />
    </Layout>
  );
};

export default SearchResults;
