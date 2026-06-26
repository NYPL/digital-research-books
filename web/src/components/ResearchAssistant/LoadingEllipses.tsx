import { Box } from "@nypl/design-system-react-components";
import React from "react";

const LoadingEllipses: React.FC = () => {
  const totalCycleTime = 0.6;
  const stagger = totalCycleTime * 0.29;
  return (
    <Box
      data-testid="assistant-loading-indicator"
      display="flex"
      justifyContent="center"
      alignItems="center"
      width="36px"
      height="36px"
      backgroundColor="#FFFFFF"
      borderRadius="50%"
      gap="6px"
    >
      {[1, 2, 3].map((index) => (
        <Box
          key={index}
          sx={{
            width: "4px",
            height: "4px",
            borderRadius: "50%",
            animation: `bounceAndColor ${totalCycleTime}s cubic-bezier(0, 0, 0.5, 0.51) infinite`,
            animationDelay: `${index * stagger}s`,
          }}
        />
      ))}
    </Box>
  );
};

export default LoadingEllipses;
