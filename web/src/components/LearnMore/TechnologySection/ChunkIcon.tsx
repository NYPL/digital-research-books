import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const ChunkIcon: React.FC = () => {
  return (
    <Icon size="2xlarge" color="transparent">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="42"
        height="42"
        viewBox="0 0 42 42"
        fill="none"
      >
        <path
          d="M12 6C12 3.23858 14.2386 1 17 1C19.7614 1 22 3.23858 22 6V9H24C26.7956 9 28.1935 9 29.2961 9.45672C30.7663 10.0657 31.9343 11.2337 32.5433 12.7039C33 13.8065 33 15.2044 33 18H36C38.7614 18 41 20.2386 41 23C41 25.7614 38.7614 28 36 28H33V31.4C33 34.7603 33 36.4405 32.346 37.7239C31.7708 38.8529 30.8529 39.7708 29.7239 40.346C28.4405 41 26.7603 41 23.4 41H22V37.5C22 35.0147 19.9853 33 17.5 33C15.0147 33 13 35.0147 13 37.5V41H10.6C7.23969 41 5.55953 41 4.27606 40.346C3.14708 39.7708 2.2292 38.8529 1.65396 37.7239C1 36.4405 1 34.7603 1 31.4V28H4C6.76142 28 9 25.7614 9 23C9 20.2386 6.76142 18 4 18H1C1 15.2044 1 13.8065 1.45672 12.7039C2.06569 11.2337 3.23373 10.0657 4.7039 9.45672C5.80653 9 7.20435 9 10 9H12V6Z"
          fill="#F9E08E"
          stroke="black"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default ChunkIcon;
