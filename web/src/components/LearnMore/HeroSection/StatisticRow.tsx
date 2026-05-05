import { Box, Flex, Text } from "@nypl/design-system-react-components";
import Divider from "../Divider";

const StatisticRow: React.FC = () => {
  return (
    <Box
      color="ui.typography.body"
      display="flex"
      flexDir="row"
      justifyContent="space-between"
      margin="0 auto"
      gap="1rem"
      marginBottom="63.5px"
    >
      <Flex flexDir="column" alignItems="center" width="181px">
        <Text
          fontSize="desktop.heading.heading1"
          fontWeight="semibold"
          lineHeight="120%"
          color="section.research.secondary"
        >
          1m
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          fontWeight="semibold"
          lineHeight="135%"
        >
          books
        </Text>
      </Flex>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Flex flexDir="column" alignItems="center" width="181px">
        <Text
          fontSize="desktop.heading.heading1"
          fontWeight="semibold"
          lineHeight="120%"
          color="section.research.secondary"
        >
          352m
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          fontWeight="semibold"
          lineHeight="135%"
        >
          pages
        </Text>
      </Flex>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Flex flexDir="column" alignItems="center" width="181px">
        <Text
          fontSize="desktop.heading.heading1"
          fontWeight="semibold"
          lineHeight="120%"
          color="section.research.secondary"
        >
          136
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          fontWeight="semibold"
          lineHeight="135%"
        >
          languages
        </Text>
      </Flex>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Flex flexDir="column" alignItems="center" width="181px">
        <Text
          fontSize="desktop.heading.heading1"
          fontWeight="semibold"
          lineHeight="120%"
          color="section.research.secondary"
        >
          82
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          fontWeight="semibold"
          lineHeight="135%"
        >
          subject
        </Text>
      </Flex>
    </Box>
  );
};

export default StatisticRow;
