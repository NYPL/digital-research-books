type ReadOnlineData = {
  read_online_url: string;
};

type DownloadData = {
  click_text: string;
  file_extension: string;
  file_name: string;
};

type UIEventData = {
  interaction?: string;
  location?: string;
  metadata_field?: string;
  metadata_value?: string;
  element_id?: string;
  [key: string]: any;
};

type EventData = {
  event: string;
} & Partial<
  ReadOnlineData &
    DownloadData &
    UIEventData & {
      item_title: string;
      item_author: string | string[];
    }
>;

export const trackEvent = (eventData: EventData) => {
  if (typeof window !== "undefined") {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push(eventData);
  }
};
