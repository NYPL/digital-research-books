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

  const breadcrumbsDataAll = breadcrumbsData
    ? [...defaultBreadcrumbs, ...breadcrumbsData]
    : defaultBreadcrumbs;

  return (
    <Breadcrumbs
      variant="research"
      breadcrumbsData={breadcrumbsDataAll}
    />
  );
};

export default DrbBreakout;
