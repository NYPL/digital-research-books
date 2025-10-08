import appConfig from "~/config/appConfig";
import { ItemReadResults } from "~/src/types/ResearchAssistant";

const apiEnv = process.env["APP_ENV"];
const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];

export const itemsReadFetcher = async (itemId: string, pageId: string) => {
  try {
    const itemsReadUrl = `${apiUrl}/items/${itemId}/read/${pageId}`;
    const res = await fetch(itemsReadUrl);
    const itemsReadResults: ItemReadResults = await res.json();

    if (res.ok) {
      return itemsReadResults;
    }
  } catch(err) {
    console.error(err);
  }
};
