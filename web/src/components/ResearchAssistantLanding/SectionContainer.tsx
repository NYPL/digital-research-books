import { Box } from "@nypl/design-system-react-components";
import React, { forwardRef } from "react";

interface SectionContainerProps {
    children: React.ReactNode;
    color?: string;
    backgroundColor?: string;
    textAlign?: "left" | "center" | "right";
    [key: string]: any; // for additional Box props
}

const SectionContainer: React.ForwardRefExoticComponent<
    SectionContainerProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, SectionContainerProps>(
    (
        {
            children,
            color = "ui.white",
            backgroundColor,
            textAlign = "center",
            ...rest
        },
        ref
    ) => (
        <Box backgroundColor={backgroundColor} paddingX="xs" ref={ref}>
            <Box
                paddingY="xxl"
                color={color}
                textAlign={textAlign}
                margin="0 auto"
                maxWidth="1280px"
                width="100%"
                {...rest}
            >
                {children}
            </Box>
        </Box>
    )
);

SectionContainer.displayName = "SectionContainer";

export default SectionContainer;
