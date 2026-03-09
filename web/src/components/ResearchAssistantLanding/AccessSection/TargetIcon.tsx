import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const TargetIcon: React.FC = () => {
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
          d="M42.6666 21.3335V13.3335L50.6666 5.3335L53.3332 10.6668L58.6666 13.3335L50.6666 21.3335H42.6666ZM42.6666 21.3335L31.9999 32M58.6667 32.0002C58.6667 46.7278 46.7276 58.6668 32 58.6668C17.2724 58.6668 5.33334 46.7278 5.33334 32.0002C5.33334 17.2726 17.2724 5.3335 32 5.3335M45.3333 32.0002C45.3333 39.364 39.3638 45.3335 32 45.3335C24.6362 45.3335 18.6667 39.364 18.6667 32.0002C18.6667 24.6364 24.6362 18.6668 32 18.6668"
          stroke="#006166"
          strokeWidth="4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default TargetIcon;
