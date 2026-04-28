import { Box, Heading, Text } from "@nypl/design-system-react-components";
import Divider from "../Divider";

const StatisticCard: React.FC = () => {
  return (
    <Box
      color="ui.typography.body"
      display="flex"
      flexDir="row"
      justifyContent="center"
      margin="0 auto"
      gap="xl"
      marginBottom="xl"
    >
      <Box display="flex" flexDir="column" alignItems="center">
        <Heading
          level="h1"
          size="heading1"
          fontWeight="bold"
          color="section.research.secondary"
        >
          1m
        </Heading>
        <Text fontSize="desktop.subtitle.subtitle1">books</Text>
      </Box>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Box display="flex" flexDir="column" alignItems="center">
        <Heading
          level="h1"
          size="heading1"
          fontWeight="bold"
          color="section.research.secondary"
        >
          352m
        </Heading>
        <Text fontSize="desktop.subtitle.subtitle1">pages</Text>
      </Box>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Box display="flex" flexDir="column" alignItems="center">
        <Heading
          level="h1"
          size="heading1"
          fontWeight="bold"
          color="section.research.secondary"
        >
          136
        </Heading>
        <Text fontSize="desktop.subtitle.subtitle1">languages</Text>
      </Box>
      <Divider orientation="vertical" color="section.research.secondary" />
      <Box display="flex" flexDir="column" alignItems="center">
        <Heading
          level="h1"
          size="heading1"
          fontWeight="bold"
          color="section.research.secondary"
        >
          82
        </Heading>
        <Text fontSize="desktop.subtitle.subtitle1">subject</Text>
      </Box>
    </Box>
  );
};

export default StatisticCard;
