import { screen, within } from "@testing-library/react";

export const FilterFormatTests = () => {
  const formats = screen.getByRole("group", { name: "Format" });
  expect(formats).toBeInTheDocument();
  expect(
    within(formats).getByRole("checkbox", { name: "Available to read" })
  ).not.toBeChecked();
  expect(
    within(formats).getByRole("checkbox", { name: "Available to download" })
  ).not.toBeChecked();
  expect(
    within(formats).getByRole("checkbox", { name: "Available to request" })
  ).not.toBeChecked();
};
