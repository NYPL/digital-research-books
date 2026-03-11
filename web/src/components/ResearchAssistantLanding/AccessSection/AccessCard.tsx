import { Box, Flex, Text } from "@nypl/design-system-react-components";

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
        background="#FCF1CB"
        borderRadius="16px"
        margin="0 auto"
        padding="m"
        width="fit-content"
      >
        {icon}
      </Box>
      <Flex flexDir="column" gap="s">
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.typography.heading"
          fontWeight="semibold"
        >
          {title}
        </Text>
        <Text color="ui.gray.dark" fontSize="desktop.subtitle.subtitle1">
          {description}
        </Text>
      </Flex>
    </Flex>
  );
};

export default AccessCard;
