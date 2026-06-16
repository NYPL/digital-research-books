import {
  Grid,
  Heading,
  Image,
  Text,
} from "@nypl/design-system-react-components";
import {
  CONTROL_SUBNAV_IMAGE,
  FEEDBACK_MESSAGE_IMAGE,
  NATURAL_LANGUAGE_MESSAGE_IMAGE,
  SOURCE_MESSAGE_IMAGE,
} from "~/src/constants/researchAssistant";
import LandingCard from "../LandingCard";
import SectionContainer from "../SectionContainer";
import CheckVerifiedIcon from "./CheckVerifiedIcon";
import LikeIcon from "./LikeIcon";
import LockIcon from "./LockIcon";
import MessageIcon from "./MessageIcon";

const HowItWorksSection: React.FC = () => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      color="ui.typography.body"
      paddingX={{ base: "none", md: "s" }}
    >
      <Heading
        level="h2"
        fontSize={{
          base: "mobile.heading.heading3",
          md: "desktop.heading.heading2",
        }}
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom="xs"
        paddingX={{ base: "s", md: "none" }}
      >
        How does Enhanced Search work?
      </Heading>
      <Text
        fontSize={{
          base: "mobile.heading.heading5",
          md: "desktop.heading.heading5",
        }}
        color="ui.gray.dark"
        fontWeight="semibold"
        marginBottom={{ base: "l", md: "xxl" }}
        paddingX={{ base: "s", md: "none" }}
      >
        The power of technology backed by the stewardship of the New York Public
        Library
      </Text>
      <Grid
        gridTemplateColumns={{
          base: "repeat(1, 1fr)",
          md: "repeat(2, 1fr)",
        }}
        gap="l"
        marginBottom={{ base: "0px", md: "xxl" }}
      >
        <LandingCard
          icon={<MessageIcon />}
          heading={
            <Heading level="h3" size="heading4">
              <Text as="span" color="section.research.secondary">
                Uses natural language
              </Text>{" "}
              so that it can understand your intent
            </Heading>
          }
          body={
            <Image
              src={NATURAL_LANGUAGE_MESSAGE_IMAGE}
              alt="A chat message from a patron asking: 'Does this book mention Smith's early life and childhood? Where in this book can I find that information?'"
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
              alt="A chat message from the VRA stating: 'Here are some books on astronomy in the middle ages.' The message includes interactive thumbs-up and thumbs-down icons for feedback."
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
              so that you can verify responses on the spot
            </Heading>
          }
          body={
            <Image
              src={SOURCE_MESSAGE_IMAGE}
              alt="A chat message from the VRA stating: 'Yes, this book mentions Smith's childhood (p3). It also describes his college years in Texas (p8).' The page numbers are styled as clickable links."
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
              alt="A subnavigation bar with two options: 'Enhanced Search' and 'Keyword search.' This illustrates how patrons can maintain control by opting out of the AI tool and switching back to traditional search at any time."
              maxWidth="512px"
              background="transparent"
              flexShrink="0"
            />
          }
        />
      </Grid>
    </SectionContainer>
  );
};

export default HowItWorksSection;
