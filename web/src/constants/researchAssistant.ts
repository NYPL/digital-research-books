export const HEADER_HEIGHT = "58px";

// ResearchAssistantLanding
export const FIND_FEATURE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/findFeature.png";
export const EVALUATE_FEATURE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/evaluateFeature.png";
export const ACCESS_FEATURE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/accessFeature.png";
export const EXPLORE_FEATURE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/exploreFeature.png";

export const FEATURES = [
  {
    featureName: "FIND",
    title: "Surface knowledge from thousands of trusted sources ",
    description:
      "The Assistant searches over 1 million digitized research books from our corpus, connecting you with trusted materials relevant to your research.",
  },
  {
    featureName: "EVALUATE",
    title: "Assess the relevance of books before reading them",
    description:
      "The Assistant provides summaries and explains why you’re seeing a particular result, so that you can quickly determine a book's usefulness upfront. ",
  },
  {
    featureName: "ACCESS",
    title: "Focus on the most important parts of a book",
    description:
      "The Assistant helps you locate specific sections in a book and cites its answers, so that you can research with efficiency and confidence.",
  },
  {
    featureName: "EXPLORE",
    title: "Expand your research by finding related content",
    description:
      "The Assistant provides recommendations based on your interests, so that you can discover more from our corpus and expand your research.",
  },
];

// ResearchAssistantPanel
const CONTENT_PADDING_VALUE = "1rem";
const OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2)";

export const MARGIN_BLEED = `calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE})`;
export const PADDING_COUNTER = `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE})`;

const MARGIN_COMPACT = "-2rem";
const PADDING_COMPACT = "l";
const MARGIN_RIGHT_COMPACT = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE}) * 2)`;

export function getPanelLayout() {
  return {
    marginX: MARGIN_COMPACT,
    paddingX: PADDING_COMPACT,
    marginRight: MARGIN_RIGHT_COMPACT,
  };
}

// ItemDetail
const GAP_SIZE = 32;
const CONTENT_TOTAL = 1280;
const NET_CONTENT = CONTENT_TOTAL - 2 * GAP_SIZE;
const NET_DISABLED = CONTENT_TOTAL - GAP_SIZE;
const ITEM_OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2 + 2rem)";
export const GRID_PADDING_X = {
  base: "1rem",
  md: "2rem",
  xl: "2rem",
};

export const getGridColumns = (vraEnabled: boolean, showChat: boolean) => {
  return vraEnabled
    ? {
        base: "1fr",
        md: showChat
          ? `
                1fr 
                ${NET_CONTENT * 0.25}px
                ${NET_CONTENT * 0.5}px
                ${NET_CONTENT * 0.25}px
                1fr
            `
          : `
                1fr 
                ${NET_CONTENT * 0.25}px
                ${NET_CONTENT * 0.65}px
                ${NET_CONTENT * 0.1}px
                1fr
            `,
      }
    : {
        base: "1fr",
        md: `
                1fr                           
                ${NET_DISABLED * 0.3333}px  
                ${NET_DISABLED * 0.6666}px    
                1fr                            
            `,
      };
};

export const getGridRows = (backUrl?: string) => {
  return backUrl ? "auto 1fr" : "1fr";
};

export const getHeaderPaddingRight = (vraEnabled: boolean) => {
  return vraEnabled ? GRID_PADDING_X : ITEM_OUTER_MARGIN_CALC;
};
