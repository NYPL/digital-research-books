type ReadOnlineData = {
  read_online_url: string;
};

type DownloadData = {
  click_text: string;
  file_extension: string;
  file_name: string;
};

type ReadOnlineOrDownloadData = (ReadOnlineData | DownloadData) & {
  event: string;
  item_title: string;
  item_author: string | string[];
};

type QueryResultsData = {
  event: string;
  query_type: string;
  results_count: number;
};

type EventData = ReadOnlineOrDownloadData | QueryResultsData;

export const trackEvent = (eventData: EventData) => {
  const dataLayer = window.dataLayer || [];
  dataLayer.push(eventData);
};
