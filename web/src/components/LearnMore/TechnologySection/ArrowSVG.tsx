import { useBreakpointValue } from "@chakra-ui/react";
import React from "react";

type ArrowDirection = "down" | "up" | "left" | "right";

type ResponsiveArrowDirection = {
  base: ArrowDirection;
  sm?: ArrowDirection;
  md?: ArrowDirection;
  lg?: ArrowDirection;
  xl?: ArrowDirection;
  "2xl"?: ArrowDirection;
};

interface ArrowSVGProps {
  direction?: ArrowDirection | ResponsiveArrowDirection;
  dashed?: boolean;
  color?: string;
  length?: number | string;
}

const rotationByDirection: Record<ArrowDirection, string> = {
  down: "0deg",
  up: "180deg",
  left: "90deg",
  right: "-90deg",
};

const ArrowSVG: React.FC<ArrowSVGProps> = ({
  direction = "down",
  dashed = false,
  color = "#006166",
  length,
}) => {
  const responsiveDirection =
    typeof direction === "string" ? { base: direction } : direction;
  const breakpointDirection = useBreakpointValue(responsiveDirection);
  const resolvedDirection = breakpointDirection ?? responsiveDirection.base;
  const isHorizontal =
    resolvedDirection === "left" || resolvedDirection === "right";
  const arrowFirst = resolvedDirection === "up" || resolvedDirection === "left";
  const resolvedLength = typeof length === "number" ? `${length}px` : length;
  const hasExplicitLength = resolvedLength !== undefined;
  const arrowHead = (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="15"
      height="9"
      viewBox="-1 0 17 10"
      fill="none"
      aria-hidden={true}
      style={{
        transform: `rotate(${rotationByDirection[resolvedDirection]})`,
        flexShrink: 0,
      }}
    >
      <path
        d="M6.65667 8.707C7.04719 9.098 7.68036 9.098 8.07088 8.707L14.4348 2.343C14.8254 1.953 14.8254 1.319 14.4348 0.929C14.0443 0.538 13.4112 0.538 13.0206 0.929L7.36378 6.586L1.70692 0.929C1.3164 0.538 0.683231 0.538 0.292707 0.929C-0.0978174 1.319 -0.0978173 1.953 0.292707 2.343L6.65667 8.707Z"
        fill={color}
        stroke={color}
      />
    </svg>
  );

  return (
    <div
      style={{
        display: "flex",
        flexDirection: isHorizontal ? "row" : "column",
        alignItems: "center",
        justifyContent: "center",
        height: isHorizontal ? "auto" : hasExplicitLength ? "auto" : "100%",
        width: isHorizontal ? (hasExplicitLength ? "auto" : "100%") : "auto",
      }}
    >
      {arrowFirst ? arrowHead : null}
      <div
        style={{
          flex: hasExplicitLength ? "0 0 auto" : 1,
          width: isHorizontal
            ? hasExplicitLength
              ? resolvedLength
              : "100%"
            : dashed
            ? 0
            : "2px",
          height: isHorizontal
            ? dashed
              ? 0
              : "2px"
            : hasExplicitLength
            ? resolvedLength
            : "100%",
          minWidth: isHorizontal ? "8px" : undefined,
          minHeight: isHorizontal ? undefined : "8px",
          marginTop: !isHorizontal && resolvedDirection === "up" ? "-5px" : 0,
          marginBottom:
            !isHorizontal && resolvedDirection === "down" ? "-5px" : 0,
          marginLeft: isHorizontal && resolvedDirection === "left" ? "-5px" : 0,
          marginRight:
            isHorizontal && resolvedDirection === "right" ? "-5px" : 0,
          backgroundColor: dashed ? "transparent" : color,
          borderTop: isHorizontal
            ? `${dashed ? "2px dashed" : "2px solid"} ${color}`
            : undefined,
          borderLeft: isHorizontal
            ? undefined
            : `${dashed ? "2px dashed" : "2px solid"} ${color}`,
        }}
      />
      {!arrowFirst ? arrowHead : null}
    </div>
  );
};

export default ArrowSVG;
