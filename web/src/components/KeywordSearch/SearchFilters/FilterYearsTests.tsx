import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Filter } from "~/src/types/SearchQuery";
import { MemoryRouter } from "next-router-mock";

export const FilterYearsTests = (
  hasApplyButton: boolean,
  startYear?: Filter,
  endYear?: Filter,
  mockRouter?: MemoryRouter
) => {
  const user = userEvent.setup({ delay: null });
  test("Renders Filter Years", () => {
    expect(
      screen.getByRole("spinbutton", {
        name: "Start",
      })
    ).toHaveValue((startYear && startYear.value) || null);
    expect(
      screen.getByRole("spinbutton", {
        name: "End",
      })
    ).toHaveValue((endYear && endYear.value) || null);
    if (hasApplyButton) {
      expect(
        screen.getByRole("button", { name: "Apply" })
      ).toBeInTheDocument();
    }
  });

  if (hasApplyButton) {
    test("Submits filters with only 'from' value", async () => {
      const yearGroup = screen.getByRole("region", {
        name: "Date filter",
      });
      const startInput = within(yearGroup).getByRole("spinbutton", {
        name: "Start",
      });
      const applyButton = within(yearGroup).getByRole("button", {
        name: "Apply",
      });
      await userEvent.type(startInput, "1990");
      expect(startInput).toHaveValue(1990);
      await user.click(applyButton);

      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          filter: "startYear:1990",
          query: "keyword:Animal Crossing",
        },
      });
    });
    test("Submits filters with only 'to' value", async () => {
      const yearGroup = screen.getByRole("region", {
        name: "Date filter",
      });
      const endInput = within(yearGroup).getByRole("spinbutton", {
        name: "End",
      });
      const applyButton = within(yearGroup).getByRole("button", {
        name: "Apply",
      });
      await userEvent.type(endInput, "1990");
      await userEvent.click(applyButton);

      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          filter: "endYear:1990",
          query: "keyword:Animal Crossing",
        },
      });
    });

    test("Submits search with both 'from' and 'to'", async () => {
      const yearGroup = screen.getByRole("region", {
        name: "Date filter",
      });
      const endInput = within(yearGroup).getByRole("spinbutton", {
        name: "End",
      });
      const startInput = within(yearGroup).getByRole("spinbutton", {
        name: "Start",
      });
      const applyButton = within(yearGroup).getByRole("button", {
        name: "Apply",
      });
      await userEvent.type(startInput, "1990");
      await userEvent.type(endInput, "2000");
      await user.click(applyButton);

      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          filter: "startYear:1990,endYear:2000",
          query: "keyword:Animal Crossing",
        },
      });
    });

    test("shows error text when 'to' is after 'from", async () => {
      const yearGroup = screen.getByRole("region", {
        name: "Date filter",
      });
      const endInput = within(yearGroup).getByRole("spinbutton", {
        name: "End",
      });
      const startInput = within(yearGroup).getByRole("spinbutton", {
        name: "Start",
      });
      const applyButton = within(yearGroup).getByRole("button", {
        name: "Apply",
      });
      await userEvent.type(startInput, "1990");
      await userEvent.type(endInput, "1890");
      await user.click(applyButton);

      expect(
        screen.getByText("Error: Start date must be before End date")
      ).toBeInTheDocument();
      expect(mockRouter).toMatchObject({});
    });
  }
};
