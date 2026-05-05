import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const LookupIcon: React.FC = () => {
  return (
    <Icon size="2xlarge" color="transparent">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="48"
        height="48"
        viewBox="0 0 48 48"
        fill="none"
      >
        <path
          d="M36 43C39.866 43 43 39.866 43 36C43 32.134 39.866 29 36 29C32.134 29 29 32.134 29 36C29 39.866 32.134 43 36 43Z"
          fill="#F9E08E"
        />
        <path
          d="M28 22H16M20 30H16M32 14H16M40 21V13.6C40 10.2397 40 8.55953 39.346 7.27606C38.7708 6.14708 37.8529 5.2292 36.7239 4.65396C35.4405 4 33.7603 4 30.4 4H17.6C14.2397 4 12.5595 4 11.2761 4.65396C10.1471 5.2292 9.2292 6.14708 8.65396 7.27606C8 8.55953 8 10.2397 8 13.6V34.4C8 37.7603 8 39.4405 8.65396 40.7239C9.2292 41.8529 10.1471 42.7708 11.2761 43.346C12.5595 44 14.2397 44 17.6 44H23M44 44L41 41M43 36C43 39.866 39.866 43 36 43C32.134 43 29 39.866 29 36C29 32.134 32.134 29 36 29C39.866 29 43 32.134 43 36Z"
          stroke="black"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default LookupIcon;
