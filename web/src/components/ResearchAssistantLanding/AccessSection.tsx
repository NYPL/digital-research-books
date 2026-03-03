import { Box, Heading, Text } from "@nypl/design-system-react-components";
import LandingButtons from "./LandingButtons";
import SectionContainer from "./SectionContainer";

interface AccessSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const AccessSection: React.FC<AccessSectionProps> = ({ heroSectionRef }) => {
  const features = [
    {
      title: "Discover relevant content",
      description:
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
    },
    {
      title: "Get oriented quickly",
      description:
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit.",
    },
    {
      title: "Find the important parts",
      description:
        "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione.",
    },
    {
      title: "Explore rich pathways",
      description:
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo.",
    },
  ];

  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      backgroundImage={`
        radial-gradient(circle, var(--nypl-colors-section-research-primary-10) 3px, transparent 3px)`}
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
      <Box
        display="grid"
        gridTemplateColumns="repeat(4, 1fr)"
        gridTemplateRows="auto"
        gap="xl"
      ></Box>
      <LandingButtons heroSectionRef={heroSectionRef} />
    </SectionContainer>
  );
};

export default AccessSection;
