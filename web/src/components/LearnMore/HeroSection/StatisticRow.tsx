import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";

const statisticsData = [
  { value: "136", label: "languages" },
  { value: "1 million", label: "books" },
  { value: "10,000+", label: "subjects" },
];

const StatisticRow: React.FC = () => {
  return (
    <Box
      color="ui.typography.body"
      display="flex"
      flexDir={{ base: "column", md: "row" }}
      justifyContent="center"
      alignItems="center"
      margin="0 auto"
      marginBottom="50.5px"
      width="100%"
    >
      {statisticsData.map((stat, index) => {
        const isMiddle = index === 1;

        return (
          <Flex
            key={stat.label}
            flexDir="column"
            alignItems="center"
            flex="1"
            maxWidth="274px"
            py={{ base: isMiddle ? "20px" : "20px", md: "0" }}
            px={{ base: "20px", md: isMiddle ? "0px" : "0" }}
            borderLeft={{ md: isMiddle ? "1px dashed" : "none" }}
            borderRight={{ md: isMiddle ? "1px dashed" : "none" }}
            borderTop={{ base: isMiddle ? "1px dashed" : "none", md: "none" }}
            borderBottom={{
              base: isMiddle ? "1px dashed" : "none",
              md: "none",
            }}
            borderColor="ui.black"
          >
            <Text
              fontSize="desktop.heading.heading1"
              fontWeight="semibold"
              lineHeight="120%"
              color="section.research.secondary"
            >
              {stat.value}
            </Text>
            <Text
              fontSize="desktop.heading.heading5"
              fontWeight="semibold"
              lineHeight="135%"
            >
              {stat.label}
            </Text>
          </Flex>
        );
      })}
    </Box>
  );
};

export default StatisticRow;
