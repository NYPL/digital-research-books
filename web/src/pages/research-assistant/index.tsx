import React from "react";
import { GetServerSideProps } from "next";
import ResearchAssistant from "~/src/components/ResearchAssistant/ResearchAssistant";
import Layout from "~/src/components/Layout/Layout";

const ResearchAssistantPage: React.FC = () => {
  return (
    <Layout>
      <ResearchAssistant />
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

export default ResearchAssistantPage;
