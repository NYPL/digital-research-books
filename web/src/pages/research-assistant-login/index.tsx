import React from "react";
import { GetServerSideProps } from "next";
import Layout from "~/src/components/Layout/Layout";
import Login from "~/src/components/ResearchAssistant/ResearchAssistantLogin";

const ResearchAssistantLoginPage: React.FC = () => {
  return (
    <Layout>
      <Login />
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

export default ResearchAssistantLoginPage;
