import { StatusBadge } from "@nypl/design-system-react-components";
import React from "react";

const PublicDomainBadge: React.FC = () => {
  return (
    <StatusBadge
      fontSize="desktop.caption"
      variant="positive"
      width="fit-content"
    >
      Public domain
    </StatusBadge>
  );
};

export default PublicDomainBadge;
