import {
  Link as DSLink,
  LinkProps as DSLinkProps,
  LinkVariants,
} from "@nypl/design-system-react-components";
import BaseLink from "next/link";
import { forwardRef } from "react";

// allow this component to accept all properties of DSLink
interface LinkProps extends DSLinkProps {
  to: any;
  modifiers?: string[];
  variant?: LinkVariants;
  hasVisitedState?: boolean;
  isUnderlined?: boolean;
}

const Link = forwardRef<HTMLAnchorElement, LinkProps>(
  (
    {
      children,
      to,
      variant,
      hasVisitedState = true,
      isUnderlined,
      "aria-label": ariaLabel,
      onClick,
      ...rest
    },
    ref
  ) => {
    return (
      <DSLink
        href={to}
        as={BaseLink}
        aria-label={ariaLabel}
        hasVisitedState={hasVisitedState}
        isUnderlined={isUnderlined}
        onClick={onClick}
        variant={variant}
        ref={ref} // Forward the ref here
        __css={{ width: "100%" }}
        {...rest}
      >
        {children}
      </DSLink>
    );
  }
);

Link.displayName = "Link";

export default Link;
