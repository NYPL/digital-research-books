import { Icon } from "@nypl/design-system-react-components";
import React from "react";

interface KeywordSearchIconProps {
    color?: string;
}

const KeywordSearchIcon: React.FC<KeywordSearchIconProps> = ({
    color,
}) => {
    const svgIcon = (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 16 16"
            fill="none"
        >
            <path
                d="M14.5 14.5L11.2375 11.2375M13 7C13 10.3137 10.3137 13 7 13C3.68629 13 1 10.3137 1 7C1 3.68629 3.68629 1 7 1C10.3137 1 13 3.68629 13 7Z"
                stroke="#006166"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );

    return (
        <Icon
            decorative
            // @ts-expect-error: Override color value type
            color={color}
            fill="none"
            size="small"
            id="keyword-search-icon"
        >
            {svgIcon}
        </Icon>
    );
};

export default KeywordSearchIcon;
