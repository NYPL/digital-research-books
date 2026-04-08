import { GetServerSideProps } from "next";
import { useRouter } from "next/router";
import React from "react";
import Layout from "~/src/components/Layout/Layout";
import ResearchAssistant from "~/src/components/ResearchAssistant/ResearchAssistant";
import VRAFeedback from "~/src/components/VRAFeedback/VRAFeedback";
import VRALayout from "~/src/components/VRALayout/VRALayout";

const ResearchAssistantPage: React.FC = () => {
  const router = useRouter();
  return (
    <Layout feedback={<VRAFeedback location={router.asPath} />}>
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
