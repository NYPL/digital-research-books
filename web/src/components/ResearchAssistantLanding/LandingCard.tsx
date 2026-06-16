import { Box, Flex } from "@nypl/design-system-react-components";
import React from "react";

interface LandingCardProps {
  icon: React.ReactNode;
  heading: React.ReactNode;
  body: React.ReactNode;
  gap?: string | { base?: string; md?: string };
}

const LandingCard: React.FC<LandingCardProps> = ({
  icon,
  heading,
  body,
  gap = { base: "m", md: "xl" },
}) => {
  return (
    <Flex
      bgColor="#FEF9EA"
      border="1px solid"
      borderColor="section.research.primary-10"
      borderRadius={{ base: "0px", md: "m" }}
      flexDir="column"
      justifyContent={{ base: "center", md: "flex-start" }}
      alignItems={{ base: "center", md: "stretch" }}
      gap={gap}
      paddingY={{ base: "m", md: "l" }}
      paddingX={{ base: "s", md: "l" }}
      textAlign={{ base: "center", md: "left" }}
      flex="1"
    >
      <Flex gap="s" flexDir="column">
        <Flex
          width="100%"
          justifyContent={{ base: "center", md: "flex-start" }}
        >
          {icon}
        </Flex>
        {heading}
      </Flex>
      <Box
        sx={{
          "> div > div": {
            maxWidth: "100%",
          },
        }}
      >
        {body}
      </Box>
    </Flex>
  );
};

export default LandingCard;
