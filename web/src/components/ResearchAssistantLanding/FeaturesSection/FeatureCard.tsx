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
      width="100%"
      backgroundColor="ui.white"
      borderRadius={{ base: "0px", md: "32px" }}
      border="1px solid"
      borderColor="section.research.primary-10"
      paddingY={{ base: "m", md: "l" }}
      paddingX={{ base: "s", md: "l" }}
    >
      <Flex
        gap={{ base: "m", md: "l" }}
        flexDir={{ base: "column", md: "row" }}
      >
        <Box flex="1">
          <Flex flexDir="column" gap={{ base: "xs", md: "s" }} textAlign="left">
            <Heading
              level="h3"
              fontSize={{
                base: "mobile.heading.heading4",
                md: "desktop.heading.heading3",
              }}
              color="ui.typography.heading"
            >
              <Flex
                gap="xs"
                alignItems="center"
                background="ui.highlighter.yellow"
                borderRadius="8px"
                color="ui.typography.body"
                fontSize="desktop.body.body1"
                marginY={{ base: "none", md: "l" }}
                marginBottom={{ base: "m", md: "l" }}
                paddingX="12px"
                paddingY="xxs"
                width="fit-content"
              >
                <ResearchAssistantIcon />
                <Text fontWeight="bold">{featureName}</Text>
              </Flex>
              <Text fontFamily="Domine">{title}</Text>
            </Heading>
            <Text
              color="ui.gray.dark"
              fontSize={{ base: "s", md: "desktop.subtitle.subtitle1" }}
            >
              {description}
            </Text>
          </Flex>
        </Box>
        <Image
          src={imageSrc}
          alt={imageAlt}
          width="820px"
          height={{ base: "auto", md: "540px" }}
          borderRadius={{ base: "8px", md: "24px" }}
          border="1px solid"
          borderColor="ui.gray.light-cool"
          flexShrink="0"
        />
      </Flex>
    </Box>
  );
};

export default FeatureCard;
