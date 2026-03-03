import { Flex } from "@nypl/design-system-react-components";
import React from "react";

interface LandingCardProps {
  icon: React.ReactNode;
  heading: React.ReactNode;
  body: React.ReactNode;
  gap?: string;
}

const LandingCard: React.FC<LandingCardProps> = ({
  icon,
  heading,
  body,
  gap = "xl",
}) => {
  return (
    <Flex
      bgColor="#FEF9EA"
      border="1px solid"
      borderColor="section.research.primary-10"
      borderRadius="24px"
      flexDir="column"
      gap={gap}
      padding="l"
      textAlign="left"
      sx={{
        ".messageBubble": {
          maxWidth: "100%",
        },
      }}
    >
      <Flex flexDir="column" gap="s">
        {icon}
        {heading}
      </Flex>
      {body}
    </Flex>
  );
};

export default LandingCard;
