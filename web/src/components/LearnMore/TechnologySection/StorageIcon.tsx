import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const StorageIcon: React.FC = () => {
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
          d="M24 16C33.9411 16 42 13.3137 42 10C42 6.68629 33.9411 4 24 4C14.0589 4 6 6.68629 6 10C6 13.3137 14.0589 16 24 16Z"
          fill="#F9E08E"
        />
        <path
          d="M42 19.4404C42 22.7604 34 25.4404 24 25.4404C14 25.4404 6 22.7604 6 19.4404"
          fill="#F9E08E"
        />
        <path
          d="M42 28.88C42 32.2 34 34.88 24 34.88C14 34.88 6 32.2 6 28.88"
          fill="#F9E08E"
        />
        <path
          d="M6 10V38C6 41.32 14 44 24 44C34 44 42 41.32 42 38V10"
          fill="#F9E08E"
        />
        <path
          d="M42 10C42 13.3137 33.9411 16 24 16C14.0589 16 6 13.3137 6 10M42 10C42 6.68629 33.9411 4 24 4C14.0589 4 6 6.68629 6 10M42 10V38C42 41.32 34 44 24 44C14 44 6 41.32 6 38V10M42 19.4404C42 22.7604 34 25.4404 24 25.4404C14 25.4404 6 22.7604 6 19.4404M42 28.88C42 32.2 34 34.88 24 34.88C14 34.88 6 32.2 6 28.88"
          stroke="black"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default StorageIcon;
