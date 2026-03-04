import { Box } from "@nypl/design-system-react-components";
import React from "react";

const LockIcon: React.FC = () => {
  return (
    <Box
      display="flex"
      minWidth="48px"
      width="48px"
      height="48px"
      backgroundColor="section.research.secondary"
      borderRadius="100%"
      alignItems="center"
      justifyContent="center"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="32"
        height="32"
        viewBox="0 0 32 32"
        fill="none"
      >
        <path
          d="M16 29.3333C21.1547 29.3333 25.3334 25.1547 25.3334 20C25.3334 14.8453 21.1547 10.6667 16 10.6667C10.8454 10.6667 6.66669 14.8453 6.66669 20C6.66669 25.1547 10.8454 29.3333 16 29.3333Z"
          fill="#F9E08E"
        />
        <path
          d="M9.46804 13.3333H9.33335V10.6667C9.33335 6.98477 12.3181 4 16 4C19.6819 4 22.6667 6.98477 22.6667 10.6667V13.3333H22.532M16 18.6667V21.3333M25.3334 20C25.3334 25.1547 21.1547 29.3333 16 29.3333C10.8454 29.3333 6.66669 25.1547 6.66669 20C6.66669 14.8453 10.8454 10.6667 16 10.6667C21.1547 10.6667 25.3334 14.8453 25.3334 20Z"
          stroke="white"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Box>
  );
};

export default LockIcon;
