import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const FileCheckIcon: React.FC = () => {
  return (
    <Icon size="3xlarge" color="transparent">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="64"
        height="64"
        viewBox="0 0 64 64"
        fill="none"
      >
        <path
          d="M53.3333 33.3335V18.1335C53.3333 13.6531 53.3333 11.4129 52.4613 9.70157C51.6944 8.19628 50.4705 6.97243 48.9652 6.20544C47.2539 5.3335 45.0137 5.3335 40.5333 5.3335H23.4666C18.9862 5.3335 16.746 5.3335 15.0347 6.20544C13.5294 6.97243 12.3056 8.19628 11.5386 9.70157C10.6666 11.4129 10.6666 13.6531 10.6666 18.1335V45.8668C10.6666 50.3472 10.6666 52.5875 11.5386 54.2987C12.3056 55.804 13.5294 57.0279 15.0347 57.7949C16.746 58.6668 18.9862 58.6668 23.4666 58.6668H32M37.3333 29.3335H21.3333M26.6666 40.0002H21.3333M42.6666 18.6668H21.3333M38.6666 50.6668L44 56.0002L56 44.0002"
          stroke="#006166"
          stroke-width="4"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default FileCheckIcon;
