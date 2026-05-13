import { Box, List, Text } from "@nypl/design-system-react-components";
import Link from "~/src/components/Link/Link";
import {
  ARCADIA_FUND_LINK,
  ARCHIVES_PORTAL_LINK,
  ASK_NYPL,
  DIGITAL_COLLECTIONS_LINK,
  ONLINE_DATABASES_LINK,
  PRIVACY_POLICY_LINK,
  RESEARCH_CATALOG_LINK,
} from "~/src/constants/links";

const generateResponseListItems = [
  <>
    <b>Continuously collect and review feedback</b> both from within the tool
    and externally from our users, librarians, and subject matter experts.
  </>,
  <>
    <b>Evaluate our training models regularly</b> to check performance and
    incorporate new improvements as they become available.
  </>,
];

const collectedDataListItems = [
  <>Timestamp of messages</>,
  <>Sources retrieved</>,
  <>Clicks on thumbs up/down buttons</>,
  <>Feedback form submissions</>,
];

const provideFeedbackListItems = [
  <>
    <b>By using the thumbs up or down buttons in the chat,</b> which will open a
    feedback form where you can provide further details if desired.
  </>,
  <>
    <b>By using the 'Help and feedback' button at the bottom of the page,</b>{" "}
    which will open up the same feedback form. Being as detailed as possible and
    providing context will help us address your concerns better.{" "}
  </>,
  <>
    <b>By completing any surveys that might appear during your session,</b>{" "}
    which will help us collect satisfaction metrics and evaluate your overall
    experience.
  </>,
];

const GENERAL_ACCORDION_DATA = [
  {
    label: "What content Enhanced Search provide access to?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Enhanced Search currently provides access to Digitized Research Books,
          a collection of over 1 million scholarly books published prior to
          1930. These come from two sources - the New York Public Library's own
          collections digitized through the Google Books project, and the public
          corpus of the{" "}
          <Link to="#" hasVisitedState={false}>
            Harvard Institutional Data Initiative
          </Link>
          .
        </Text>
        <Text>
          At this time, Enhanced Search does not provide access to other parts
          of the NYPL Research Collections, such as materials from the{" "}
          <Link to={RESEARCH_CATALOG_LINK} hasVisitedState={false}>
            Research Catalog
          </Link>
          ,{" "}
          <Link to={DIGITAL_COLLECTIONS_LINK} hasVisitedState={false}>
            Digital Collections
          </Link>
          ,{" "}
          <Link to={ONLINE_DATABASES_LINK} hasVisitedState={false}>
            Online Resource & Databases
          </Link>
          , or the{" "}
          <Link to={ARCHIVES_PORTAL_LINK} hasVisitedState={false}>
            Archives Portal
          </Link>
          . It also does not search the NYPL{" "}
          <Link to="#" hasVisitedState={false}>
            Circulating Catalog
          </Link>{" "}
          or the greater web.
        </Text>
      </Box>
    ),
  },
  {
    label: "Is Enhanced Search free to use?",
    panel: (
      <Box>
        Yes, Enhanced Search is free to use. It can be used without an NYPL
        account or library card. You do not need to be logged in to use the
        tool.
      </Box>
    ),
  },
  {
    label: "How do I use Enhanced Search?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Enhanced Search is an AI-enabled tool that uses a natural language
          chat interface to help you discover and access content from over 1
          million scholarly books published prior to 1930. You can use it both
          to search <i>for</i> and <i>within</i> these books.
        </Text>
        <Text marginBottom="s">
          Start by asking a question or typing in a topic of interest and view
          results in the left sidebar. You can continue to refine your search by
          asking follow-up questions like “Show me only books published between
          1890 and 1920.” Every result also includes a “Why am I seeing this
          result?” section to help you understand the tool's reasoning.
        </Text>
        <Text>
          Once you've selected a book, you can use the chat to help you locate
          relevant content from that book. You can ask questions like “Does this
          book mention the contributions of Copernicus to 16th century
          astronomy?” and the tool will guide you to the parts containing that
          information.
        </Text>
      </Box>
    ),
  },
  {
    label: "What types of content does Digitized Research Books have?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Digitized Research Books contains over 1 million scholarly books
          published prior to 1930. This humanities-focused collection, largely
          in English and other Western European languages, spans subjects such
          as literature, law, history, poetry, fiction, and more.
        </Text>
        <Text>
          The composition of the collection will continue to change as we add
          more books. In the future, we hope to provide access to a greater
          variety of languages, subjects, and time periods.
        </Text>
      </Box>
    ),
  },
];

const securityAccordionData = [
  {
    label: "What is done with the information I type into Enhanced Search?",
    panel: (
      <Box>
        <Text marginBottom="s">
          When you type something into Enhanced Search, our AI models turn it
          into embeddings. Embeddings are mathematical representations of your
          questions that can be read by the technology to understand your
          intent. These are matched against the collection to find and show you
          relevant content.
        </Text>
        <Text marginBottom="s">
          Your questions are kept confidential. While we don't record your
          conversation history, it will be available to you until you close or
          refresh your browser, or for 30 days of leaving it open. There is no
          way for another user, your school, your employer, or NYPL to see your
          conversations with the tool.
        </Text>
        <Text>
          View our{" "}
          <Link to={PRIVACY_POLICY_LINK} hasVisitedState={false}>
            privacy policy
          </Link>
          .
        </Text>
      </Box>
    ),
  },
  {
    label: "What security measures do you use to safeguard my data?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Enhanced Search does not have access to any personally identifiable
          information. It cannot identify you apart from any information you
          voluntarily provide, even when you use it when logged in from your
          NYPL account. Additionally, clicking on 'Start over' at any point
          erases your conversation history and ensures no memory retention.
        </Text>
        <Text marginBottom="s">
          Everything you type into the tool is sent to our AI models over HTTPS
          and validated with a secure API. All conversations are encrypted with
          Transport Layer Security (TLS) to preserve your privacy.
        </Text>
        <Text>
          View our{" "}
          <Link to={PRIVACY_POLICY_LINK} hasVisitedState={false}>
            privacy policy
          </Link>
          .
        </Text>
      </Box>
    ),
  },
  {
    label: "What data do you collect?",
    panel: (
      <Box>
        <Text marginBottom="s">
          We collect the following usage statistics in order to evaluate and
          improve the tool:
        </Text>
        <Box marginLeft="s" marginBottom="s">
          <List
            listItems={collectedDataListItems}
            variant="ul"
            sx={{
              "li::before": {
                content: '"+" / ""',
                fontWeight: "medium",
                color: "ui.black",
              },
            }}
          />
        </Box>
        <Text marginBottom="s">
          These usage statistics are stored in a database and used to identify
          patterns and troubleshoot problems. They are visible only to a limited
          number of NYPL staff. They are anonymous and cannot be traced to your
          name or institution.
        </Text>
        <Text>
          View our{" "}
          <Link to={PRIVACY_POLICY_LINK} hasVisitedState={false}>
            privacy policy
          </Link>
          .
        </Text>
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
          <Link to={PRIVACY_POLICY_LINK} hasVisitedState={false}>
            privacy policy
          </Link>
          .
        </Text>
      </Box>
    ),
  },
  {
    label: "Can I opt out of using Enhanced Search or turn it off?",
    panel: (
      <Box>
        Yes, you can opt out of using Enhanced Search and still access Digitized
        Research Books through a traditional keyword search. To do this, click
        on the 'Keyword search' tab at the top of the search results page, or
        turn the 'Use Enhanced Search' tool toggle off on the book page.
      </Box>
    ),
  },
];

const TECHNOLOGY_ACCORDION_DATA = [
  {
    label: "Why did NYPL develop Enhanced Search?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Enhanced Search was developed in service of NYPL's mission to support
          open access to scholarly content in a global context. Made possible
          through the generous support of{" "}
          <Link to={ARCADIA_FUND_LINK} hasVisitedState={false}>
            The Arcadia Fund
          </Link>
          , it was born out of the Library's commitment to innovation and its
          enduring values of trust and authenticity.
        </Text>
        <Text>
          Currently in beta and live on Digitized Research Books, Enhanced
          Search leverages cutting-edge advances in AI to transform how users
          interact with our resources and ushers in a new paradigm of discovery
          and access. As NYPL's first public-facing venture enabled by AI, it is
          designed to meaningfully support researchers while adhering to the
          high legal, ethical, and academic standards that govern our
          institution.
        </Text>
      </Box>
    ),
  },
  {
    label: "How does Enhanced Search generate its responses?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Enhanced Search uses a Large Language Model (LLM) to understand your
          questions and generate responses. It uses a technology called
          Retrieval Augmented Generation (RAG) to find relevant materials from
          the collection based on your intent. By using RAG, we ensure that only
          vetted sources are searched, minimizing the chances of hallucinations
          and mistakes. To maintain quality and improve accuracy we also:
        </Text>
        <Box marginLeft="s">
          <List
            listItems={generateResponseListItems}
            variant="ul"
            sx={{
              "li::before": {
                content: '"+" / ""',
                fontWeight: "medium",
                color: "ui.black",
              },
            }}
          />
        </Box>
      </Box>
    ),
  },
  {
    label: "Can Enhanced Search hallucinate or make mistakes?",
    panel: (
      <Box>
        <Text marginBottom="s">
          As with any AI-powered tool, there is always some risk of
          hallucination (defined as when a chatbot generates untrue information
          in its response) when using Enhanced Search.
        </Text>
        <Text>
          Though the tool is designed to only provide answers grounded in
          trusted academic sources, you are encouraged to verify its responses
          and report to us if you discover an error. AI can make mistakes, and
          the evaluation of the output is ultimately in the hands of the user.
        </Text>
      </Box>
    ),
  },
  {
    label: "How does Enhanced Search differ from other AI research tools?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Unlike many other AI research tools that are behind institutional or
          subscription paywalls, Enhanced Search provides free access to our
          collection of Digitized Research Books without requiring institutional
          affiliation or even an NYPL library card.
        </Text>
        <Text>
          Enhanced Search is designed to support content discovery rather than
          content generation. It uses AI to expand access to our vetted
          collections, and to connect researchers to scholarly books quickly and
          efficiently. It aims to augment the rigor of the research process, not
          replace it.
        </Text>
      </Box>
    ),
  },
  {
    label: "What languages does Enhanced Search support?",
    panel: (
      <Box>
        <Text marginBottom="s">
          While Enhanced Search is able to interpret conversations in multiple
          languages, the output (which will be generated in the language of the
          input) may vary in quality depending on the complexity of the input.
          For best results, we recommend communicating with the tool in English.
        </Text>
        <Text marginBottom="s">
          The Digitized Research Books collection contains books in several
          languages. These may be shown in the results if relevant, even when
          the input is in English. Some titles may be transliterated due to the
          original characters not being supported by the tool.
        </Text>
        <Text>
          Enhanced Search cannot provide direct text translation of source
          material.
        </Text>
      </Box>
    ),
  },
  {
    label: "Can I access my previous conversations?",
    panel: (
      <Box>
        No, Enhanced Search does not retain your previous conversations. If you
        wish to keep your chats for future reference, we recommend copying and
        pasting them into a word document.
      </Box>
    ),
  },
];

const COST_ACCORDION_DATA = [
  {
    label: "Who pays for Enhanced Search?",
    panel: (
      <Box>
        Enhanced Search is made possible through the generous support of{" "}
        <Link to={ARCADIA_FUND_LINK} hasVisitedState={false}>
          The Arcadia Fund
        </Link>
        . Their sponsorship has enabled NYPL to invest in AI to improve and
        expand access to scholarly content, and to make available this tool to
        patrons free of charge.
      </Box>
    ),
  },
  {
    label: "How does Enhanced Search manage the environmental impacts of AI?",
    panel: (
      <Box>
        <Text marginBottom="s">
          NYPL recognizes the environmental impacts of AI and is committed to
          using it ethically and responsibly. We understand that while AI can
          help advance meaningful access to scholarship, it comes with high
          energy demands. Our goal is to build a lean, scalable, and efficient
          search tool that deploys AI selectively and with restraint.
        </Text>
        <Text marginBottom="s">
          Energy use is an important factor in our model selection. Our models
          have been chosen after careful consideration of their carbon footprint
          alongside other factors such as task suitability, cost, speed, and
          overall performance. We continue to monitor the tool's energy
          consumption and re-evaluate our models regularly.
        </Text>
        <Text>
          <Link
            to="#"
            color="section.research.secondary"
            hasVisitedState={false}
          >
            Learn more
          </Link>{" "}
          about the project.
        </Text>
      </Box>
    ),
  },
];

const HELP_ACCORDION_DATA = [
  {
    label: "How do I provide feedback about Enhanced Search?",
    panel: (
      <Box>
        <Text marginBottom="s">
          Your feedback is important and helps us improve the tool. You can
          provide it in the following ways:
        </Text>
        <Box marginLeft="s" marginBottom="s">
          <List
            listItems={provideFeedbackListItems}
            variant="ul"
            sx={{
              "li::before": {
                content: '"+" / ""',
                fontWeight: "medium",
                color: "ui.black",
              },
            }}
          />
        </Box>
        <Text>
          All feedback remains anonymous unless you choose to voluntarily
          include any identifying information. Providing an email address is
          optional but helps us get in touch with you if needed.
        </Text>
      </Box>
    ),
  },
  {
    label: "What if I need help using this tool or have more questions?",
    panel: (
      <Box>
        We are happy to assist if you have any additional questions or need help
        using the tool. Please{" "}
        <Link to={ASK_NYPL} hasVisitedState={false}>
          contact us
        </Link>{" "}
        with your question and an email address, and we will reach out to you as
        soon as possible.
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
