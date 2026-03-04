import { Box } from "@nypl/design-system-react-components";
import React from "react";

const MessageIcon: React.FC = () => {
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
          d="M27.9995 15.3333C27.9995 21.5926 22.9254 26.6667 16.6662 26.6667C15.2305 26.6667 13.8571 26.3997 12.593 25.9127C12.3619 25.8237 12.2464 25.7792 12.1545 25.758C12.0641 25.7371 11.9987 25.7284 11.906 25.7249C11.8117 25.7213 11.7084 25.732 11.5016 25.7534L4.67355 26.4592C4.02256 26.5265 3.69706 26.5601 3.50506 26.443C3.33782 26.341 3.22392 26.1706 3.1936 25.977C3.15879 25.7548 3.31433 25.4669 3.62541 24.8911L5.80629 20.8544C5.98589 20.522 6.07569 20.3558 6.11637 20.1959C6.15653 20.0381 6.16625 19.9243 6.1534 19.7619C6.14039 19.5975 6.06825 19.3835 5.92399 18.9555C5.54062 17.8181 5.33283 16.6 5.33283 15.3333C5.33283 9.07411 10.4069 4 16.6662 4C22.9254 4 27.9995 9.07411 27.9995 15.3333Z"
          fill="#F9E08E"
          stroke="white"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Box>
  );
};

export default MessageIcon;
