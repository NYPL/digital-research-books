import { Box, List, Text } from "@nypl/design-system-react-components";
import Link from "~/src/components/Link/Link";
import { RESEARCH_CATALOG_LINK } from "~/src/constants/links";

const generateResponseListItems = [
  <>
    <Text as="span" isBold>
      Continuously collect and review feedback
    </Text>{" "}
    both from within the tool and externally from our users, librarians, and
    subject matter experts.
  </>,
  <>
    <Text as="span" isBold>
      Evaluate our training models regularly
    </Text>{" "}
    to check performance and incorporate new improvements as they become
    available.
  </>,
];

const GENERAL_ACCORDION_DATA = [
  {
    label:
      "What content does the Virtual Research Assistant provide access to?",
    panel: (
      <Box>
        <Text marginBottom="s">
          The Virtual Research Assistant currently provides access to over 1
          million public domain books from two sources - books from NYPL
          digitized through the Google Books project, and books from the
          <Link to="#" hasVisitedState={false}>
            Harvard Institutional Data Initiative
          </Link>
          . More books are added to the repository on a regular basis.
        </Text>
        <Text>
          At this time, the Virtual Research Assistant does not provide access
          to other parts of the NYPL Research Collections, such as materials
          from the{" "}
          <Link
            to={RESEARCH_CATALOG_LINK}
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Research Catalog
          </Link>
          ,{" "}
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Digital Collections
          </Link>
          ,{" "}
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Online Databases
          </Link>
          , or the{" "}
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Archives Portal
          </Link>
          . It also does not search the greater web.
        </Text>
      </Box>
    ),
  },
  {
    label: "Is the Virtual Research Assistant free to use? ",
    panel: (
      <Box>
        Yes, the Virtual Research Assistant is free to use. It can be accessed
        without an NYPL account or library card. You do not need to be logged in
        to use the tool.
      </Box>
    ),
  },
  {
    label: "How do I use the Virtual Research Assistant?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "What types of books are in the current repository?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
];

const securityAccordionData = [
  {
    label:
      "What is done with the information I type into the Virtual Research Assistant?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "What data do you collect?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "Will my personal information be tracked, stored, or sold?",
    panel: (
      <Box>
        <Text marginBottom="s">
          We do not track, store, or sell your personal information. We do not
          use your data to train the tool. We track usage by IP address to
          ensure compliance with copyright laws, monitor abuse, and detect bot
          activity or unusual traffic spikes.
        </Text>
        <Text>
          View our{" "}
          <Link to="#" color="section.research.secondary">
            privacy policy
          </Link>
          .
        </Text>
      </Box>
    ),
  },
  {
    label:
      "Can I opt out of using the Virtual Research Assistant, or turn it off?",
    panel: (
      <Box>
        Yes, you can opt out of using the Virtual Research Assistant and still
        access the repository by using a traditional keyword search. The tool
        can be turned off by clicking on the &apos;keyword search&apos; tab at
        the top of the search results page, or by moving the toggle to the
        &apos;off&apos; position on the book page.
      </Box>
    ),
  },
];

const TECHNOLOGY_ACCORDION_DATA = [
  {
    label: "Why did NYPL develop the Virtual Research Assistant?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "How does the Virtual Research Assistant generate its responses?",
    panel: (
      <Box>
        <Text marginBottom="s">
          The Virtual Research Assistant uses a Large Language Model (LLM) to
          understand your questions and generate its responses. It uses a
          technology called Retrieval Augmented Generation (RAG) to find
          relevant materials from the repository. RAG ensures that only vetted
          sources are searched, minimizing the chances of hallucinations and
          mistakes. To maintain quality and improve accuracy we also:
        </Text>
        <Box marginLeft="s" marginBottom="s">
          <List
            listItems={generateResponseListItems}
            variant="ul"
            sx={{
              "li::before": {
                content: '"+"',
                fontWeight: "medium",
                color: "ui.black",
              },
            }}
          />
        </Box>
        <Text>
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Learn more about the project.
          </Link>
        </Text>
      </Box>
    ),
  },
  {
    label: "What AI models does the Virtual Research Assistant use?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "Can the Virtual Research Assistant hallucinate or make mistakes?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label:
      "How does the Virtual Research Assistant differ from other AI research tools?What data do you collect?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "What input languages does the Virtual Research Assistant support?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "Can I access my previous conversations?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
];

const COST_ACCORDION_DATA = [
  {
    label: "Who pays for the Virtual Research Assistant?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label:
      "How does the Virtual Research Assistant manage the environmental impacts of AI?",
    panel: (
      <Box>
        <Text marginBottom="s">
          NYPL recognizes the environmental impacts of AI and is committed to
          using it ethically and responsibly. We understand that while AI can
          help advance meaningful access to scholarship, it also comes with high
          energy demands. Our goal is to build a lean, scalable, and efficient
          research tool that uses AI only on an &apos;as needed&apos; basis to
          reduce resource consumption.
        </Text>
        <Text marginBottom="s">
          Energy use is an important factor in our model selection. Our current
          models have been chosen after careful consideration of their
          environmental impact alongside other factors such as task suitability,
          cost, and performance. We continue to monitor the Virtual Research
          Assistant&apos;s energy usage and re-evaluate our models regularly.
        </Text>
        <Text>
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Learn more about the project.
          </Link>
        </Text>
      </Box>
    ),
  },
];

const HELP_ACCORDION_DATA = [
  {
    label: "How do I provide feedback about the Virtual Research Assistant?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
  {
    label: "What if I need help using this tool or have more questions?",
    panel: (
      <Box>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
        veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea
        commodo consequat. Duis aute irure dolor in reprehenderit in voluptate
        velit esse cillum dolore eu fugiat nulla pariatur.
      </Box>
    ),
  },
];

export const ACCORDION_SECTIONS = [
  { title: "General", data: GENERAL_ACCORDION_DATA },
  { title: "Security and privacy", data: securityAccordionData },
  { title: "Technology and capabilities", data: TECHNOLOGY_ACCORDION_DATA },
  { title: "Cost and impact", data: COST_ACCORDION_DATA },
  { title: "Help and feedback", data: HELP_ACCORDION_DATA },
];
