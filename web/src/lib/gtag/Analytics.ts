type EventData = (ReadOnlineData | DownloadData) & {
    event: string;
    item_title: string;
    item_author: string | string[];
};

type ReadOnlineData = {
    read_online_url: string;
}

type DownloadData = {
    click_text: string;
    file_extension: string;
    file_name: string;
}

export const trackEvent = (eventData: EventData) => {
    const dataLayer = window.dataLayer || [];
    dataLayer.push(eventData);
};
