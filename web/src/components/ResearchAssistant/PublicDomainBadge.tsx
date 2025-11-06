import React from "react";
import { StatusBadge } from "@nypl/design-system-react-components";

const PublicDomainBadge: React.FC = () => {
  return (
    <StatusBadge
      fontSize="desktop.caption"
      variant="positive"
      width={{ base: "100%", md: "fit-content" }}
    >
      PUBLIC DOMAIN
    </StatusBadge>
  );
};

export default PublicDomainBadge;
