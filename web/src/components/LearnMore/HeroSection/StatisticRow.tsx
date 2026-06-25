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
      marginBottom={{ base: "12px", sm: "l", md: "50.5px" }}
      width="100%"
    >
      {statisticsData.map((stat, index) => (
        <React.Fragment key={stat.label}>
          <Flex
            flexDir="column"
            alignItems="center"
            flex="1"
            maxWidth="274px"
            py={{ base: "20px", sm: "0" }}
            px={{ base: "20px", sm: "0" }}
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

          {index < statisticsData.length - 1 && (
            <Box
              flexShrink={0}
              color="section.research.secondary"
              width={{ base: "96px", sm: "1px" }}
              height={{ base: "1px", sm: "96px" }}
              backgroundImage={{
                base:
                  "repeating-linear-gradient(to right, currentColor 0 4px, transparent 4px 8px)",
                sm:
                  "repeating-linear-gradient(to bottom, currentColor 0 4px, transparent 4px 8px)",
              }}
            />
          )}
        </React.Fragment>
      ))}
    </Box>
  );
};

export default StatisticRow;
