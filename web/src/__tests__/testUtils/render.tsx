import { render, RenderOptions, RenderResult } from "@testing-library/react";
import { ReactElement } from "react";
import { FeatureFlagProvider } from "~/src/context/FeatureFlagContext";
import { FeedbackProvider } from "~/src/context/FeedbackContext";
import { ResearchAssistantProvider } from "~/src/context/ResearchAssistantContext";

const Providers = ({ children }) => {
  return (
    <FeedbackProvider>
      <FeatureFlagProvider>{children}</FeatureFlagProvider>
    </FeedbackProvider>
  );
};

const customRender = (
  ui: ReactElement,
  options?: Omit<RenderOptions, "wrapper">
): RenderResult => {
  return render(ui, { wrapper: Providers, ...options });
};

const renderWithResearchAssistant = (
  component: ReactElement,
  options?: Omit<RenderOptions, "wrapper">
): RenderResult => {
  const Providers = ({ children }) => (
    <FeedbackProvider>
      <ResearchAssistantProvider>{children}</ResearchAssistantProvider>
    </FeedbackProvider>
  );

  return render(component, { wrapper: Providers, ...options });
};

export * from "@testing-library/react";
export { customRender as render, renderWithResearchAssistant };
