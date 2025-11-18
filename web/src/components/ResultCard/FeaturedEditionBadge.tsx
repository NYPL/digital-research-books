import React from "react";
import { StatusBadge } from "@nypl/design-system-react-components";

const FeaturedEditionBadge: React.FC = () => {
  return (
    <StatusBadge
      fontSize="desktop.caption"
      variant="recommendation"
      width={{ base: "100%", md: "fit-content" }}
    >
      Featured edition
    </StatusBadge>
  );
};

export default FeaturedEditionBadge;
