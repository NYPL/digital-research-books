import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";

interface AccessCardProps {
  icon: React.ReactNode;
  title: string;
  description: string;
}

const AccessCard: React.FC<AccessCardProps> = ({
  icon,
  title,
  description,
}) => {
  return (
    <Flex flexDir="column" gap="l">
      <Box
        background="ui.white"
        border="2px solid"
        borderColor="section.research.secondary"
        borderRadius="16px"
        margin="0 auto"
        padding="s"
        width="fit-content"
      >
        {icon}
      </Box>
      <Flex flexDir="column" gap="s">
        <Heading level="h3" size="heading5">
          {title}
        </Heading>
        <Text color="ui.gray.dark" fontSize="desktop.subtitle.subtitle1">
          {description}
        </Text>
      </Flex>
    </Flex>
  );
};

export default AccessCard;
