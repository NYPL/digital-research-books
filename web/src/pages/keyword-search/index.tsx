import React from "react";
import Layout from "~/src/components/Layout/Layout";
import { ApiSearchQuery } from "../../types/SearchQuery";
import { searchResultsFetcher } from "../../lib/api/SearchApi";
import { toSearchQuery } from "~/src/util/apiConversion";
import Error from "../_error";
import KeywordSearch from "~/src/components/KeywordSearch/KeywordSearch";
import VRALayout from "~/src/components/VRALayout/VRALayout";

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
  if (props.searchResults.status !== 200) {
    return <Error statusCode={props.searchResults.status} />;
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
          searchResults={props.searchResults}
        />
      </VRALayout>
    </Layout>
  );
};

export default SearchResults;
