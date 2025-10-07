import React from "react";
import { GetServerSideProps } from "next";
import Layout from "~/src/components/Layout/Layout";
import KeywordSearchLanding from "~/src/components/KeywordSearchLanding/KeywordSearchLanding";
import VRALayout from "~/src/components/VRALayout/VRALayout";

const KeywordSearchLandingPage: React.FC = () => {
  return (
    <Layout>
      <VRALayout
        activePage="keyword"
        breadcrumbsData={[{ url: "/keyword-search", text: "Keyword Search" }]}
      >
        <KeywordSearchLanding />
      </VRALayout>
    </Layout>
  );
};

export const getServerSideProps: GetServerSideProps = async () => {
  const isResearchAssistantEnabled = process.env.APP_ENV !== "production";

  if (!isResearchAssistantEnabled) {
    return {
      notFound: true,
    };
  }

  return {
    props: {},
  };
};

export default KeywordSearchLandingPage;
