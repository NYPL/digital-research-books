import { Box } from "@nypl/design-system-react-components";
import React from "react";

const LightbulbIcon: React.FC = () => {
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
          d="M20 20.435C23.1533 18.937 25.3334 15.723 25.3334 11.9998C25.3334 6.84518 21.1547 2.6665 16 2.6665C10.8454 2.6665 6.66669 6.84518 6.66669 11.9998C6.66669 15.723 8.84676 18.937 12 20.435V21.3332C12 22.5757 12 23.1969 12.203 23.687C12.4737 24.3404 12.9928 24.8595 13.6462 25.1302C14.1363 25.3332 14.7575 25.3332 16 25.3332C17.2425 25.3332 17.8638 25.3332 18.3538 25.1302C19.0072 24.8595 19.5264 24.3404 19.797 23.687C20 23.1969 20 22.5757 20 21.3332V20.435Z"
          fill="#F9E08E"
        />
        <path
          d="M12.6667 29.3332H19.3334M20 20.435C23.1533 18.937 25.3334 15.723 25.3334 11.9998C25.3334 6.84518 21.1547 2.6665 16 2.6665C10.8454 2.6665 6.66669 6.84518 6.66669 11.9998C6.66669 15.723 8.84676 18.937 12 20.435V21.3332C12 22.5757 12 23.1969 12.203 23.687C12.4737 24.3404 12.9928 24.8595 13.6462 25.1302C14.1363 25.3332 14.7575 25.3332 16 25.3332C17.2425 25.3332 17.8638 25.3332 18.3538 25.1302C19.0072 24.8595 19.5264 24.3404 19.797 23.687C20 23.1969 20 22.5757 20 21.3332V20.435Z"
          stroke="white"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Box>
  );
};

export default LightbulbIcon;
