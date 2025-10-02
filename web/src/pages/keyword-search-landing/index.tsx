import React from "react";
import { GetServerSideProps } from "next";
import Layout from "~/src/components/Layout/Layout";
import KeywordSearchLanding from "~/src/components/KeywordSearchLanding/KeywordSearchLanding";

const KeywordSearchLandingPage: React.FC = () => {
  return (
    <Layout>
      <KeywordSearchLanding />
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
