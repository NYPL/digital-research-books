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
  variant?: LinkVariants;
  isUnderlined?: boolean;
}

const Link = ({
  children,
  to,
  variant,
  isUnderlined,
  "aria-label": ariaLabel,
  onClick,
  ...rest
}: IProps) => {
  return (
    <DSLink
      href={to}
      as={BaseLink}
      aria-label={ariaLabel}
      isUnderlined={isUnderlined}
      onClick={onClick}
      variant={variant}
      __css={{ width: "100%" }}
      {...rest}
    >
      {children}
    </DSLink>
  );
};

Link.displayName = "Link";

export default Link;
