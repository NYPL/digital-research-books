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
      flexDir={{ base: "column", sm: "row" }}
      justifyContent="center"
      alignItems="center"
      margin="0 auto"
      marginBottom={{ base: "l", md: "50.5px" }}
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
            py={{ base: isMiddle ? "20px" : "20px", sm: "0" }}
            px={{ base: "20px", sm: isMiddle ? "0px" : "0" }}
            borderLeft={{ sm: isMiddle ? "1px dashed" : "none" }}
            borderRight={{ sm: isMiddle ? "1px dashed" : "none" }}
            borderTop={{ base: isMiddle ? "1px dashed" : "none", sm: "none" }}
            borderBottom={{
              base: isMiddle ? "1px dashed" : "none",
              sm: "none",
            }}
            borderColor="ui.black"
          >
            <Text
              fontSize={{
                base: "mobile.heading.heading1",
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
                base: "mobile.heading.heading5",
                md: "desktop.heading.heading5",
              }}
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
