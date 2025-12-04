import { Icon, IconProps } from "@nypl/design-system-react-components";
import React from "react";

type ArrowDirection = "right" | "left" | "up" | "down";

const directionToRotation: Record<ArrowDirection, IconProps["iconRotation"]> = {
    right: "rotate0",
    down: "rotate90",
    left: "rotate180",
    up: "rotate270",
};

interface ArrowIconProps {
    direction?: ArrowDirection;
}

const ArrowIcon: React.FC<ArrowIconProps> = ({ direction = "right" }) => {
    return (
        <Icon
            size="medium"
            color="transparent"
            iconRotation={directionToRotation[direction]}
        >
            <svg
                xmlns="http://www.w3.org/2000/svg"
                width="18"
                height="18"
                viewBox="0 0 18 18"
                fill="none"
            >
                <path
                    d="M3.75 9H14.25M14.25 9L9 3.75M14.25 9L9 14.25"
                    stroke="white"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                />
            </svg>
        </Icon>
    );
};

export default ArrowIcon;
