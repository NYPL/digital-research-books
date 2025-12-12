import { Icon, IconProps } from "@nypl/design-system-react-components";
import React from "react";

type ArrowDirection = "right" | "left" | "up" | "down";

const directionToRotation: Record<ArrowDirection, IconProps["iconRotation"]> = {
    right: "rotate0",
    down: "rotate270",
    left: "rotate90",
    up: "rotate180",
};

interface ArrowIconProps {
    direction?: ArrowDirection;
    color?: string;
}

const ArrowIcon: React.FC<ArrowIconProps> = ({ direction = "right", color = "ui.white" }) => {
    return (
        <Icon
            size="medium"
            fill={color}
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
                    stroke={color}
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                />
            </svg>
        </Icon>
    );
};

export default ArrowIcon;
