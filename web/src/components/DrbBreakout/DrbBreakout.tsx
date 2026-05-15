import { Breadcrumbs } from "@nypl/design-system-react-components";
import { BreadcrumbsDataProps } from "@nypl/design-system-react-components/dist/src/components/Breadcrumbs/Breadcrumbs";
import React from "react";
import { defaultBreadcrumbs } from "~/src/constants/labels";

const DrbBreakout: React.FC<{
  children?: React.ReactNode;
  breadcrumbsData?: BreadcrumbsDataProps[];
}> = ({ children, breadcrumbsData }) => {
  return (
    <>
      <DrbBreadcrumbs breadcrumbsData={breadcrumbsData} />
      {children}
    </>
  );
};

const DrbBreadcrumbs: React.FC<{ breadcrumbsData: BreadcrumbsDataProps[] }> = (
  props
) => {
  const { breadcrumbsData } = props;

  let breadcrumbsDataAll = breadcrumbsData
    ? [...defaultBreadcrumbs, ...breadcrumbsData]
    : defaultBreadcrumbs;

  const isDigitizedContext = breadcrumbsDataAll.some(
    (crumb) =>
      crumb.text === "Enhanced Search (beta)" || crumb.text === "Learn more"
  );

  if (isDigitizedContext) {
    breadcrumbsDataAll = breadcrumbsDataAll.map((crumb) => {
      if (crumb.text === "Digital Research Books Beta") {
        return {
          ...crumb,
          text: "Digitized Research Books",
        };
      }
      return crumb;
    });
  }

  return (
    <Breadcrumbs variant="research" breadcrumbsData={breadcrumbsDataAll} />
  );
};

export default DrbBreakout;
