import {
  Accordion,
  Box,
  Heading,
  List,
  Text,
} from "@nypl/design-system-react-components";
import { RESEARCH_CATALOG_LINK } from "~/src/constants/links";
import Link from "../Link/Link";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
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

  const accordionData = [
    {
      label: "What corpus does the Virtual Research Assistant search over?",
      panel: (
        <Box>
          <Text marginBottom="s">
            The Virtual Research Assistant currently searches over a corpus of
            over 1 million{" "}
            <Text as="span" isBold>
              digitized research books in the public domain
            </Text>
            , with more books being added to every month. It uses a combination
            of Artificial Intelligence (AI) and Machine Learning (ML)
            technologies to scan these books and surface relevant content based
            on your prompts or questions.
          </Text>

          <Text>
            At this time, the Virtual Research Assistant does not provide access
            to other parts of the NYPL Research Collections, such as materials
            from the{" "}
            <Link to={RESEARCH_CATALOG_LINK} color="section.research.secondary">
              Research Catalog
            </Link>
            ,{" "}
            <Link to="#" color="section.research.secondary">
              Digital Collections
            </Link>
            ,{" "}
            <Link to="#" color="section.research.secondary">
              Online Databases
            </Link>
            , or the{" "}
            <Link to="#" color="section.research.secondary">
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
          without an NYPL account or library card. You do not need to be logged
          in to use the tool.
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
            <Link to="#" color="section.research.secondary">
              Learn more about the project.
            </Link>
          </Text>
        </Box>
      ),
    },
    {
      label:
        "Will my personal information be stored or used to train the Virtual Research Assistant?",
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
        "How does the Virtual Research Assistant manage the environmental impacts of AI?",
      panel: (
        <Box>
          <Text marginBottom="s">
            NYPL recognizes the environmental impacts of AI and is committed to
            using it ethically and responsibly. We understand that while AI can
            help advance meaningful access to scholarship, it also comes with
            high energy demands. Our goal is to build a lean, scalable, and
            efficient research tool that uses AI only on an &apos;as
            needed&apos; basis to reduce resource consumption.
          </Text>
          <Text marginBottom="s">
            Energy use is an important factor in our model selection. Our
            current models have been chosen after careful consideration of their
            environmental impact alongside other factors such as task
            suitability, cost, and performance. We continue to monitor the
            Virtual Research Assistant&apos;s energy usage and re-evaluate our
            models regularly.
          </Text>
          <Text>
            <Link to="#" color="section.research.secondary">
              Learn more about the project.
            </Link>
          </Text>
        </Box>
      ),
    },
    {
      label: "What if I don't want to use the Virtual Research Assistant?",
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

  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
    >
      <Heading
        level="h2"
        size="heading2"
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom="xxl"
      >
        Frequently asked questions
      </Heading>
      <Accordion
        backgroundColor="ui.white"
        color="ui.black"
        textAlign="left"
        id="faq-accordion"
        accordionData={accordionData}
        sx={{
          button: {
            fontWeight: "bold",
          },
          "button:focus": {
            outlineColor: "section.research.secondary",
          },
          "button[aria-expanded=true]": {
            bgColor: "section.research.secondary",
            color: "ui.white",
          },
          "button[aria-expanded=true]:hover": {
            bgColor: "section.research.primary",
          },
          "button[aria-expanded=false]": {
            bgColor: "ui.white",
            color: "section.research.secondary",
          },
          "button[aria-expanded=false]:hover": {
            bgColor: "section.research.primary-10",
          },
          ".chakra-collapse": {
            bgColor: "ui.white",
          },
        }}
      />
    </SectionContainer>
  );
};

export default FaqSection;
