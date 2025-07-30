import React from "react";
import { GetServerSideProps } from "next";
import Layout from "~/src/components/Layout/Layout";
import ResearchAssistantLanding from "~/src/components/ResearchAssistant/ResearchAssistantLanding";

const ResearchAssistantLandingPage: React.FC = () => {
  return (
    <Layout>
      <ResearchAssistantLanding />
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

export default ResearchAssistantLandingPage;
