import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const TargetIcon: React.FC = () => {
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
          d="M37.9999 4L31.9999 10V16H37.9999L43.9999 10L39.9999 8L37.9999 4Z"
          fill="#F9E08E"
        />
        <path d="M31.9999 16L23.9999 23.9999L31.9999 16Z" fill="#F9E08E" />
        <path
          d="M31.9999 16V10L37.9999 4L39.9999 8L43.9999 10L37.9999 16H31.9999ZM31.9999 16L23.9999 23.9999M44 24C44 35.0457 35.0457 44 24 44C12.9543 44 4 35.0457 4 24C4 12.9543 12.9543 4 24 4M34 24C34 29.5228 29.5228 34 24 34C18.4772 34 14 29.5228 14 24C14 18.4772 18.4772 14 24 14"
          stroke="black"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default TargetIcon;
