import React from "react";
import { StatusBadge } from "@nypl/design-system-react-components";

const PhysicalEditionBadge: React.FC = () => {
  return (
    <StatusBadge
      fontSize="desktop.caption"
      variant="informative"
      width={{ base: "100%", md: "fit-content" }}
    >
      Physical edition
    </StatusBadge>
  );
};

export default PhysicalEditionBadge;
