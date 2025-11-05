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
            width="20"
            height="20"
            viewBox="0 0 20 20"
            fill="none"
        >
            <path
                d="M9.75 0.75L11.4842 5.25886C11.7662 5.99209 11.9072 6.35871 12.1265 6.66709C12.3208 6.9404 12.5596 7.17919 12.8329 7.37353C13.1413 7.5928 13.5079 7.73381 14.2411 8.01582L18.75 9.75L14.2411 11.4842C13.5079 11.7662 13.1413 11.9072 12.8329 12.1265C12.5596 12.3208 12.3208 12.5596 12.1265 12.8329C11.9072 13.1413 11.7662 13.5079 11.4842 14.2411L9.75 18.75L8.01582 14.2411C7.73381 13.5079 7.5928 13.1413 7.37353 12.8329C7.17919 12.5596 6.9404 12.3208 6.66709 12.1265C6.35871 11.9072 5.99209 11.7662 5.25886 11.4842L0.75 9.75L5.25886 8.01582C5.99209 7.73381 6.35871 7.5928 6.66709 7.37353C6.9404 7.17919 7.17919 6.9404 7.37353 6.66709C7.5928 6.35871 7.73381 5.99209 8.01582 5.25886L9.75 0.75Z"
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
                minWidth="36px"
                width="36px"
                height="36px"
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
