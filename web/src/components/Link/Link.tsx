import React from "react";
import BaseLink from "next/link";
import {
  Link as DSLink,
  LinkVariants,
} from "@nypl/design-system-react-components";

// allow this component to accept all properties of "a" tag
interface IProps extends React.AnchorHTMLAttributes<HTMLAnchorElement> {
  to: any;
  modifiers?: string[];
  linkVariant?: LinkVariants;
  isUnderlined?: boolean;
}

const Link = ({
  children,
  to,
  linkVariant,
  isUnderlined,
  "aria-label": ariaLabel,
  onClick,
}: IProps) => {
  return (
    <DSLink
      href={to}
      as={BaseLink}
      isUnderlined={isUnderlined}
      variant={linkVariant}
      onClick={onClick}
      aria-label={ariaLabel}
      __css={{ width: "100%" }}
    >
      {children}
    </DSLink>
  );
};

Link.displayName = "Link";

export default Link;
