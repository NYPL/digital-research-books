import { Grid, TextInputRefType } from "@nypl/design-system-react-components";
import AccessCard from "../AccessSection/AccessCard";
import LandingButtons from "../LandingButtons";
import SectionContainer from "../SectionContainer";
import AwardIcon from "./AwardIcon";
import BuildingIcon from "./BuildingIcon";
import DataFlowIcon from "./DataFlowIcon";
import GraduationHatIcon from "./GraduationHatIcon";
import OpenBookIcon from "./OpenBookIcon";
import TargetIcon from "./TargetIcon";

interface PrinciplesSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
}

const PrinciplesSection: React.FC<PrinciplesSectionProps> = ({
  heroSectionRef,
  textInputRef,
}) => {
  const accessCardData = [
    {
      icon: <BuildingIcon />,
      title: "Grounded in authoritative sources",
      description:
        "We train the tool to only search our collection of trusted scholarly books to ensure that all  its responses are verifiable.",
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
        "We regularly evaluate our models and technical frameworks to ensure that the tool's outputs align with our values.",
    },
    {
      icon: <TargetIcon />,
      title: "Designed for deep engagement",
      description:
        "We strive to connect you with scholarly sources quickly and efficiently so that you can free up time for deep analysis.",
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
        "We solicit feedback from the academic community to ensure that the tool meets researchers where they are",
    },
  ];

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
      <Grid
        gridTemplateColumns={{
          base: "repeat(1, 1fr)",
          sm: "repeat(2, 1fr)",
          md: "repeat(3, 1fr)",
        }}
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

export default PrinciplesSection;
