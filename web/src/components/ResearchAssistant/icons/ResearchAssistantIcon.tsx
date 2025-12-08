import { Box, Icon, IconSizes } from "@nypl/design-system-react-components";
import React from "react";

interface ResearchAssistantIconProps {
    color?: string;
    inCircle?: boolean;
    size?: IconSizes;
}

const ResearchAssistantIcon: React.FC<ResearchAssistantIconProps> = ({
    color,
    inCircle = false,
    size = "medium",
}) => {
    const svgIcon = (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="18"
            height="18"
            viewBox="0 0 18 18"
            fill="none"
        >
            <path
                d="M9 2.25L10.3006 5.63165C10.5121 6.18157 10.6179 6.45653 10.7824 6.68781C10.9281 6.8928 11.1072 7.07189 11.3122 7.21765C11.5435 7.3821 11.8184 7.48786 12.3684 7.69937L15.75 9L12.3684 10.3006C11.8184 10.5121 11.5435 10.6179 11.3122 10.7824C11.1072 10.9281 10.9281 11.1072 10.7824 11.3122C10.6179 11.5435 10.5121 11.8184 10.3006 12.3684L9 15.75L7.69937 12.3684C7.48786 11.8184 7.3821 11.5435 7.21765 11.3122C7.07189 11.1072 6.8928 10.9281 6.68781 10.7824C6.45653 10.6179 6.18157 10.5121 5.63165 10.3006L2.25 9L5.63165 7.69937C6.18157 7.48786 6.45653 7.3821 6.68781 7.21765C6.8928 7.07189 7.07189 6.8928 7.21765 6.68781C7.3821 6.45653 7.48786 6.18157 7.69937 5.63165L9 2.25Z"
                fill="#006166"
                stroke="#006166"
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );

    if (!inCircle) {
        return (
            <Icon
                decorative
                // @ts-expect-error: Override color value type
                color={color}
                size={size}
                id="research-assistant-icon"
            >
                {svgIcon}
            </Icon>
        );
    } else {
        return (
            <Box
                display="flex"
                minWidth="24px"
                width="24px"
                height="24px"
                backgroundColor="#E6F3F3"
                borderRadius="100%"
                alignItems="center"
                justifyContent="center"
            >
                {svgIcon}
            </Box>
        );
    }
};

export default ResearchAssistantIcon;
