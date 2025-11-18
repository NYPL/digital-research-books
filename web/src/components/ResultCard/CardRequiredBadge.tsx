import React from "react";
import { StatusBadge } from "@nypl/design-system-react-components";

const CardRequiredBadge: React.FC = () => {
  return (
    <StatusBadge
      fontSize="desktop.caption"
      variant="warning"
      width={{ base: "100%", md: "fit-content" }}
    >
      Library card required
    </StatusBadge>
  );
};

export default CardRequiredBadge;
