import React from "react";
import { GetServerSideProps } from "next";
import Layout from "~/src/components/Layout/Layout";
import ResearchAssistantLanding from "~/src/components/ResearchAssistant/ResearchAssistantLanding";
import { useRouter } from "next/router";

const ResearchAssistantLandingPage: React.FC = () => {
  const router = useRouter();

  const handleSubmit = (query: string) => {
    sessionStorage.setItem("researchAssistantInitialMessage", query.trim());
    router.push("/research-assistant");
  };

  return (
    <Layout>
      <ResearchAssistantLanding onSubmit={handleSubmit} />
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
