import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const ChunkIcon: React.FC = () => {
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
          d="M15 9C15 6.23858 17.2386 4 20 4C22.7614 4 25 6.23858 25 9V12H27C29.7956 12 31.1935 12 32.2961 12.4567C33.7663 13.0657 34.9343 14.2337 35.5433 15.7039C36 16.8065 36 18.2044 36 21H39C41.7614 21 44 23.2386 44 26C44 28.7614 41.7614 31 39 31H36V34.4C36 37.7603 36 39.4405 35.346 40.7239C34.7708 41.8529 33.8529 42.7708 32.7239 43.346C31.4405 44 29.7603 44 26.4 44H25V40.5C25 38.0147 22.9853 36 20.5 36C18.0147 36 16 38.0147 16 40.5V44H13.6C10.2397 44 8.55953 44 7.27606 43.346C6.14708 42.7708 5.2292 41.8529 4.65396 40.7239C4 39.4405 4 37.7603 4 34.4V31H7C9.76142 31 12 28.7614 12 26C12 23.2386 9.76142 21 7 21H4C4 18.2044 4 16.8065 4.45672 15.7039C5.06569 14.2337 6.23373 13.0657 7.7039 12.4567C8.80653 12 10.2044 12 13 12H15V9Z"
          fill="#F9E08E"
          stroke="black"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default ChunkIcon;
