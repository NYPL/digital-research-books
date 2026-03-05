import { Box } from "@nypl/design-system-react-components";
import React, { forwardRef } from "react";

interface SectionContainerProps {
  children: React.ReactNode;
  color?: string;
  backgroundColor?: string;
  textAlign?: "left" | "center" | "right";
  borderColor?: string;
  borderTop?: string;
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
      borderColor,
      borderTop,
      ...rest
    },
    ref
  ) => (
    <Box
      backgroundColor={backgroundColor}
      borderTop={borderTop}
      borderColor={borderColor}
      paddingX="xs"
      ref={ref}
      {...rest}
    >
      <Box
        paddingY="xxl"
        color={color}
        textAlign={textAlign}
        margin="0 auto"
        maxWidth="1280px"
        width="100%"
      >
        {children}
      </Box>
    </Box>
  )
);

SectionContainer.displayName = "SectionContainer";

export default SectionContainer;
