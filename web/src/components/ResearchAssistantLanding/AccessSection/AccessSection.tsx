import {
  Grid,
  Heading,
  Image,
  Text,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import {
  CONTROL_SUBNAV_IMAGE,
  FEEDBACK_MESSAGE_IMAGE,
  NATURAL_LANGUAGE_MESSAGE_IMAGE,
  SOURCE_MESSAGE_IMAGE,
} from "~/src/constants/researchAssistant";
import LandingButtons from "../LandingButtons";
import LandingCard from "../LandingCard";
import QuoteSection from "../QuoteSection";
import SectionContainer from "../SectionContainer";
import AccessCard from "./AccessCard";
import AwardIcon from "./AwardIcon";
import BuildingIcon from "./BuildingIcon";
import CheckVerifiedIcon from "./CheckVerifiedIcon";
import DataFlowIcon from "./DataFlowIcon";
import GraduationHatIcon from "./GraduationHatIcon";
import LikeIcon from "./LikeIcon";
import LockIcon from "./LockIcon";
import MessageIcon from "./MessageIcon";
import OpenBookIcon from "./OpenBookIcon";
import TargetIcon from "./TargetIcon";

interface AccessSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
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
    icon: <AwardIcon />,
    title: "Backed by rigorous quality checks",
    description:
      "We regularly evaluate our technical frameworks and the tool's outputs to ensure quality and accuracy.",
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

const AccessSection: React.FC<AccessSectionProps> = ({
  heroSectionRef,
  textInputRef,
}) => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
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
      <Grid gridTemplateColumns="repeat(2, 1fr)" gap="l" marginBottom="xxl">
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
            <Image
              src={NATURAL_LANGUAGE_MESSAGE_IMAGE}
              alt="A chat message from a patron asking: 'Does this book make any mention of the Smith's early life and childhood? Which parts of the book can I find this info in?'"
              maxWidth="512px"
              background="transparent"
              flexShrink="0"
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
            <Image
              src={FEEDBACK_MESSAGE_IMAGE}
              alt="A chat message from the VRA stating: 'Here are some results that match your criteria. Verify results. Your data is not used to train our models.' The message includes interactive thumbs-up and thumbs-down icons for feedback."
              maxWidth="512px"
              background="transparent"
              flexShrink="0"
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
            <Image
              src={SOURCE_MESSAGE_IMAGE}
              alt="A chat message from the VRA stating: 'Yes, this book mentions Smith's birth and childhood (p3). It also describes his early schooling in Kansas (p8).' The page numbers are styled as clickable links."
              maxWidth="512px"
              background="transparent"
              flexShrink="0"
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
            <Image
              src={CONTROL_SUBNAV_IMAGE}
              alt="A subnavigation bar with two options: 'Virtual Research Assistant' and 'Keyword search.' This illustrates how patrons can maintain control by opting out of the AI tool and switching back to traditional search at any time."
              maxWidth="512px"
              background="transparent"
              flexShrink="0"
            />
          }
        />
      </Grid>
      <QuoteSection />
      <Grid
        gridTemplateColumns="repeat(3, 1fr)"
        rowGap="xxl"
        columnGap="l"
        marginTop="xxl"
      >
        {accessCardData.map((card, index) => (
          <AccessCard
            key={index}
            icon={card.icon}
            title={card.title}
            description={card.description}
          />
        ))}
      </Grid>
      <LandingButtons
        heroSectionRef={heroSectionRef}
        textInputRef={textInputRef}
      />
    </SectionContainer>
  );
};

export default AccessSection;
