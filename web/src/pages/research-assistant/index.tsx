import React from "react";
import { GetServerSideProps } from "next";
import ResearchAssistant from "~/src/components/ResearchAssistant/ResearchAssistant";
import Layout from "~/src/components/Layout/Layout";
import VRALayout from "~/src/components/VRALayout/VRALayout";

const ResearchAssistantPage: React.FC = () => {
  return (
    <Layout>
      <VRALayout
        activePage="vra"
        breadcrumbsData={[
          { url: "/research-assistant", text: "Virtual Research Assistant" },
        ]}
      >
        <ResearchAssistant />
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

export default ResearchAssistantPage;
