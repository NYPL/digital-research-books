import { ItemType, MessageItem, MessageRole } from "../types/ResearchAssistant";

export const SURVEY_SESSION_STORAGE_KEY = "vra-popup-survey-handled";
export const SURVEY_DELAY_MS = 180000;

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
      "Enhanced Search uses a natural language chat interface to engage with over 1 million Digitized Research Books, so that you can quickly find relevant scholarly content.",
    imageSrc: FIND_FEATURE_IMAGE,
    imageAlt:
      'Split-screen interface with search results on the left and the Enhanced Search chat on the right. A patron asks about US copyright law history. The AI confirms matching items found and prompts the patron to ask follow up questions or choose from suggested options including "Digital Copyright Act," "Intellectual property law," and "Fair use doctrine."',
  },
  {
    featureName: "EVALUATE",
    title: "Assess the relevance of books before reading them",
    description:
      "Enhanced Search provides summaries and explains why you're seeing a particular result, so that you can determine a book's usefulness before diving into it. ",
    imageSrc: EVALUATE_FEATURE_IMAGE,
    imageAlt:
      "Split-screen interface showing an e-book search result for 'Notions and fancy foods v.49' on the left, and a chat window requesting information about English department stores on the right. The search result's 'Why am I seeing this result?' explanation connects the book to the chat topic.",
  },
  {
    featureName: "ACCESS",
    title: "Focus on the most important parts of a book",
    description:
      "Enhanced Search directs you to the most relevant sections of a book and cites its sources, so that you can research with efficiency and confidence.",
    imageSrc: ACCESS_FEATURE_IMAGE,
    imageAlt:
      "PDF viewer showing a biography of Charles Thomas Walker. In the AI assistant on the right the patron asked if the book mentions Walker's early life and childhood and the assistant replies with confirmation and cites page 2. The document text details his 1858 birth in Hephzibah, Georgia, as the youngest of 11 children.",
  },
  {
    featureName: "EXPLORE",
    title: "Expand your research by finding related content",
    description:
      "Enhanced Search provides recommendations based on your interests, so that you can discover more materials and find new information pathways.",
    imageSrc: EXPLORE_FEATURE_IMAGE,
    imageAlt:
      "PDF viewer showing a book cover. The AI assistant on the right suggests related content, including 'Life of Charles T. Walker, D.D.' by Silas Xavier Floyd and 'The first Colored Baptist church in North America' by James Simms.",
  },
];

// ResearchAsistantWindow
export const CATALOG_INITIAL_MESSAGE: MessageItem = {
  type: ItemType.Message,
  role: MessageRole.Assistant,
  content: [
    {
      text: "What research topic can I help you explore today?",
      type: "output_text",
    },
  ],
};
export const CONTENT_INITIAL_MESSAGE: MessageItem = {
  type: ItemType.Message,
  role: MessageRole.Assistant,
  content: [
    {
      text:
        "I can help you find relevant content in this book. Ask me a question, or try the suggestions below.",
      type: "output_text",
    },
  ],
};
export const LOADING_MESSAGE: MessageItem = {
  type: ItemType.Message,
  role: MessageRole.Assistant,
  content: [
    {
      text: "Thinking... This may take several seconds.",
      type: "output_text",
    },
  ],
};

export const NATURAL_LANGUAGE_MESSAGE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/naturalLanguageMessage.png";
export const FEEDBACK_MESSAGE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/feedbackMessage.png";
export const SOURCE_MESSAGE_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/sourceMessage.png";
export const CONTROL_SUBNAV_IMAGE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/controlSubnav.png";

export const NATURAL_LANGUAGE_MESSAGE_IMAGE_TABLET =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/naturalLanguageMessage-tablet.png";
export const FEEDBACK_MESSAGE_IMAGE_TABLET =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/feedbackMessage-tablet.png";
export const SOURCE_MESSAGE_IMAGE_TABLET =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/sourceMessage-tablet.png";
export const CONTROL_SUBNAV_IMAGE_TABLET =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/controlSubnav-tablet.png";

export const NATURAL_LANGUAGE_MESSAGE_IMAGE_MOBILE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/naturalLanguageMessage-mobile.png";
export const FEEDBACK_MESSAGE_IMAGE_MOBILE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/feedbackMessage-mobile.png";
export const SOURCE_MESSAGE_IMAGE_MOBILE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/sourceMessage-mobile.png";
export const CONTROL_SUBNAV_IMAGE_MOBILE =
  "https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/controlSubnav-mobile.png";

// ResearchAssistantPanel
const CONTENT_PADDING_VALUE = { base: "1rem", md: "2rem" };
const OUTER_MARGIN_CALC = "max(0px, calc((100vw - 1280px) / 2))";

export const MARGIN_BLEED = {
  base: "0",
  md: `calc(calc(${OUTER_MARGIN_CALC} * -1))`,
};
export const PADDING_COUNTER = {
  base: `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE.base})`,
  md: `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE.md})`,
};

const MARGIN_COMPACT = { base: "-1rem", md: "-2rem" };
const PADDING_COMPACT = { base: "m", md: "l" };
const PADDING_COUNTER_COMPACT = {
  base: "s",
  md: `calc(calc(${OUTER_MARGIN_CALC} * 2) + ${CONTENT_PADDING_VALUE.md})`,
};

export function getPanelLayout() {
  return {
    marginX: MARGIN_COMPACT,
    paddingX: PADDING_COMPACT,
    paddingRight: PADDING_COUNTER_COMPACT,
    marginRight: MARGIN_BLEED,
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

// SnippetList
export const SNIPPETS_PER_PAGE = 6;
