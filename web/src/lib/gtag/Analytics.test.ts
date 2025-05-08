import { trackEvent } from "./Analytics";

describe("Google Analytics", () => {
  beforeEach(() => {
    window.dataLayer = [];
  });

  describe("trackEvent", () => {
    test("it should update window.dataLayer with the approriate values", () => {
      const dataLayer = window.dataLayer;

      trackEvent({
        "event":  "file_download",
        "click_text": "Download PDF",
        "file_extension": "pdf",
        "file_name": "File Name",
        "item_title": "Item Title",
        "item_author": "Item Author",
      });

      const eventData = dataLayer[0];
      const eventValue = eventData.event;
      const clickText = eventData.click_text;
      const fileExtension = eventData.file_extension;
      const fileName = eventData.file_name;
      const itemTitle = eventData.item_title;
      const itemAuthor = eventData.item_author;

      expect(dataLayer).toHaveLength(1);
      expect(eventValue).toEqual("file_download");
      expect(clickText).toEqual("Download PDF");
      expect(fileExtension).toEqual("pdf");
      expect(fileName).toEqual("File Name");
      expect(itemTitle).toEqual("Item Title");
      expect(itemAuthor).toEqual("Item Author");
    });
  });
});
