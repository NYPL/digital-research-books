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
      minHeight="602px"
      width="100%"
      backgroundColor="ui.white"
      borderRadius="32px"
      border="1px solid"
      borderColor="section.research.primary-10"
      padding="l"
    >
      <Flex gap="l">
        <Box flex="1">
          <Flex flexDir="column" gap="s" textAlign="left">
            <Heading level="h3" size="heading3" color="ui.typography.heading">
              <Flex
                gap="xs"
                alignItems="center"
                background="ui.highlighter.yellow"
                borderRadius="8px"
                color="ui.typography.body"
                fontSize="desktop.body.body1"
                marginTop="l"
                marginBottom="l"
                paddingX="12px"
                paddingY="xxs"
                width="fit-content"
              >
                <ResearchAssistantIcon />
                <Text fontWeight="bold">{featureName}</Text>
              </Flex>
              <Text fontFamily="Domine">{title}</Text>
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
          borderRadius="24px"
          border="1px solid"
          borderColor="ui.gray.light-cool"
          flexShrink="0"
        />
      </Flex>
    </Box>
  );
};

export default FeatureCard;
