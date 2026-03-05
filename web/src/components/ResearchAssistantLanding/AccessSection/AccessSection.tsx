import {
  Flex,
  Grid,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import {
  ItemType,
  MessageRole,
  MessageStatus,
} from "~/src/types/ResearchAssistant";
import KeywordSearchIcon from "../../ResearchAssistant/icons/KeywordSearchIcon";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import MessageBubble from "../../ResearchAssistant/MessageBubble";
import LandingButtons from "../LandingButtons";
import LandingCard from "../LandingCard";
import SectionContainer from "../SectionContainer";
import AccessCard from "./AccessCard";
import BuildingIcon from "./BuildingIcon";
import CheckVerifiedIcon from "./CheckVerifiedIcon";
import DataFlowIcon from "./DataFlowIcon";
import FileCheckIcon from "./FileCheckIcon";
import GraduationHatIcon from "./GraduationHatIcon";
import LikeIcon from "./LikeIcon";
import LockIcon from "./LockIcon";
import MessageIcon from "./MessageIcon";
import OpenBookIcon from "./OpenBookIcon";
import TargetIcon from "./TargetIcon";

interface AccessSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const accessCardData = [
  {
    icon: <BuildingIcon />,
    title: "Grounded in authoritative sources",
    description:
      "We only search within our corpus of digitized research books to ensure that all responses are trustworthy and verifiable.",
  },
  {
    icon: <OpenBookIcon />,
    title: "Built in partnership with librarians",
    description:
      "We collaborate with our staff and experts to build features and flows that meet the needs of real-world researchers.",
  },
  {
    icon: <FileCheckIcon />,
    title: "Backed by rigorous quality checks",
    description:
      "We regularly evaluate our technical frameworks and the tool’s outputs to ensure quality and accuracy.",
  },
  {
    icon: <TargetIcon />,
    title: "Designed for deep engagement",
    description:
      "We strive to connect you with scholarly sources as fast as possible so that you can free up time for deeper analysis.",
  },
  {
    icon: <GraduationHatIcon />,
    title: "Underpinned by academic integrity",
    description:
      "We're committed to using AI to enhance and democratize access to scholarly research - not replace or undermine it.",
  },
  {
    icon: <DataFlowIcon />,
    title: "Developed for research workflows",
    description:
      "We solicit feedback from the academic community so that we can continue to enhance the research experience.",
  },
];

const AccessSection: React.FC<AccessSectionProps> = ({ heroSectionRef }) => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      backgroundImage={`
        radial-gradient(circle, var(--nypl-colors-section-research-primary-10) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      color="ui.typography.body"
    >
      <Heading
        level="h2"
        size="heading2"
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom="xs"
      >
        How does the Assistant work?
      </Heading>
      <Text
        color="ui.gray.dark"
        fontSize="desktop.heading.heading5"
        fontWeight="semibold"
        marginBottom="xxl"
      >
        The power of technology backed by the stewardship of the New York Public
        Library
      </Text>
      <Grid gridTemplateColumns="repeat(2, 1fr)" gap="l">
        <LandingCard
          icon={<MessageIcon />}
          heading={
            <Heading level="h3" size="heading4">
              <Text as="span" color="section.research.secondary">
                Uses natural language
              </Text>{" "}
              so you&apos;re not worried about crafting the perfect query
            </Heading>
          }
          body={
            <MessageBubble
              message={{
                type: ItemType.Message,
                role: MessageRole.User,
                content:
                  "Does this book make any mention of the Smith's early life and childhood? Which parts of the book can I find this info in?",
                id: "user-message-1",
                status: MessageStatus.Sending,
              }}
              index={0}
            />
          }
        />
        <LandingCard
          icon={<LikeIcon />}
          heading={
            <Heading level="h3" size="heading4">
              <Text as="span" color="section.research.secondary">
                Improves with your feedback
              </Text>{" "}
              to deliver more relevant results
            </Heading>
          }
          body={
            <MessageBubble
              message={{
                type: ItemType.Message,
                role: MessageRole.Assistant,
                content: [
                  {
                    type: "output_text",
                    text: "Here are some results that match your criteria.",
                  },
                ],
                id: "assistant-message-1",
                status: MessageStatus.Sending,
              }}
              index={1}
            />
          }
        />
        <LandingCard
          icon={<CheckVerifiedIcon />}
          heading={
            <Heading level="h3" size="heading4">
              <Text as="span" color="section.research.secondary">
                Shows you the source
              </Text>{" "}
              so that you can verify its responses on the spot
            </Heading>
          }
          body={
            <MessageBubble
              message={{
                type: ItemType.Message,
                role: MessageRole.Assistant,
                content: [
                  {
                    type: "output_text",
                    text:
                      "Yes, this book mentions Smith's birth and childhood (p3). It also describes his early schooling in Kansas. (p8).",
                  },
                ],
                id: "assistant-message-2",
                status: MessageStatus.Sending,
              }}
              index={1}
            />
          }
        />
        <LandingCard
          icon={<LockIcon />}
          heading={
            <Heading level="h3" size="heading4">
              <Text as="span" color="section.research.secondary">
                Keeps you in control
              </Text>{" "}
              by letting you opt out of the tool at any time
            </Heading>
          }
          body={
            <Flex
              gap="xs"
              backgroundColor="ui.white"
              alignItems="center"
              border="1px solid"
              borderColor="ui.border.default"
              borderRadius="8px"
              paddingX="s"
              paddingY="m"
            >
              <Flex
                alignItems="center"
                gap="xxs"
                backgroundColor="ui.white"
                color="ui.typography.body"
                paddingX="s"
                paddingY="xxs"
                _hover={{
                  backgroundColor: "transparent",
                }}
              >
                <ResearchAssistantIcon />
                <Text>Virtual Research Assistant</Text>
              </Flex>
              <Flex
                gap="xxs"
                backgroundColor="section.research.primary-05"
                border="1px solid"
                borderColor="section.research.primary"
                borderRadius="6px"
                color="section.research.secondary"
                fontWeight="semibold"
                paddingX="s"
                paddingY="xxs"
              >
                <KeywordSearchIcon color="transparent" />
                Keyword search
              </Flex>
            </Flex>
          }
        />
      </Grid>
      <Text
        fontSize="desktop.heading.heading1"
        fontFamily="Domine"
        marginY="128px"
        lineHeight="1"
      >
        <Text>Our mission is to use</Text>
        <Text>technology responsibly to</Text>
        <Text color="section.research.secondary">
          expand access to research
        </Text>
      </Text>
      <Grid gridTemplateColumns="repeat(3, 1fr)" rowGap="xxl" columnGap="l">
        {accessCardData.map((card, index) => (
          <AccessCard
            key={index}
            icon={card.icon}
            title={card.title}
            description={card.description}
          />
        ))}
      </Grid>
      <LandingButtons heroSectionRef={heroSectionRef} />
    </SectionContainer>
  );
};

export default AccessSection;
