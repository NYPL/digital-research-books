import { GetServerSideProps } from "next";
import React from "react";
import Layout from "~/src/components/Layout/Layout";
import ResearchAssistant from "~/src/components/ResearchAssistant/ResearchAssistant";
import VRAFeedback from "~/src/components/VRAFeedback/VRAFeedback";
import VRALayout from "~/src/components/VRALayout/VRALayout";

const ResearchAssistantPage: React.FC = () => {
  return (
    <Layout feedback={<VRAFeedback />}>
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
