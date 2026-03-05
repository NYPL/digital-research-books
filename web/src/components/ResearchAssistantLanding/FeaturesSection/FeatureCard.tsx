import {
  Box,
  Flex,
  Heading,
  Image,
  Text,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";

interface FeatureCardProps {
  featureName: string;
  title: string;
  description: string;
  imageSrc: string;
  imageAlt: string;
}

const FeatureCard: React.FC<FeatureCardProps> = ({
  featureName,
  title,
  description,
  imageSrc,
  imageAlt,
}) => {
  return (
    <Box
      height="602px"
      width="100%"
      backgroundColor="ui.white"
      borderRadius="32px"
      border="1px solid"
      borderColor="section.research.primary-10"
      padding="l"
    >
      <Flex gap="l">
        <Box flex="1">
          <Flex
            gap="xs"
            alignItems="center"
            background="ui.highlighter.yellow"
            borderRadius="8px"
            marginBottom="l"
            paddingX="12px"
            paddingY="xxs"
            width="fit-content"
          >
            <ResearchAssistantIcon />
            <Text fontWeight="bold">{featureName}</Text>
          </Flex>
          <Flex flexDir="column" gap="s" textAlign="left">
            <Heading
              level="h3"
              fontFamily="Domine"
              size="heading3"
              color="ui.typography.heading"
            >
              {title}
            </Heading>
            <Text color="ui.gray.dark" fontSize="desktop.subtitle.subtitle1">
              {description}
            </Text>
          </Flex>
        </Box>
        <Image
          src={imageSrc}
          alt={imageAlt}
          width="820px"
          height="540px"
          borderRadius="32px"
          border="1px solid"
          borderColor="ui.gray.light-cool"
          flexShrink="0"
        />
      </Flex>
    </Box>
  );
};

export default FeatureCard;
