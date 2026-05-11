import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import Divider from "../Divider";

const statisticsData = [
  { value: "1m", label: "books" },
  { value: "352m", label: "pages" },
  { value: "136", label: "languages" },
  { value: "82", label: "subjects" },
];

const StatisticRow: React.FC = () => {
  return (
    <Box
      color="ui.typography.body"
      display="flex"
      flexDir="row"
      justifyContent="center"
      alignItems="center"
      margin="0 auto"
      gap={{ base: "xs", md: "1rem" }}
      marginBottom="63.5px"
      width="100%"
    >
      {statisticsData.map((stat, index) => (
        <React.Fragment key={stat.label}>
          <Flex flexDir="column" alignItems="center" flex="1" maxWidth="181px">
            <Text
              fontSize={{
                base: "desktop.heading.heading2",
                md: "desktop.heading.heading1",
              }}
              fontWeight="semibold"
              lineHeight="120%"
              color="section.research.secondary"
            >
              {stat.value}
            </Text>
            <Text
              fontSize={{
                base: "desktop.body.body1",
                md: "desktop.heading.heading5",
              }}
              fontWeight="semibold"
              lineHeight="135%"
            >
              {stat.label}
            </Text>
          </Flex>

          {index < statisticsData.length - 1 && (
            <Divider
              orientation="vertical"
              color="section.research.secondary"
            />
          )}
        </React.Fragment>
      ))}
    </Box>
  );
};

export default StatisticRow;
