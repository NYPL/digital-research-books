import appConfig from "~/config/appConfig";
import { ItemReadResults } from "~/src/types/ResearchAssistant";

const apiEnv = process.env["APP_ENV"];
const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];

export const itemsReadFetcher = async (readId: string) => {
  try {
    // TODO: Make item_id not hard-coded (123)
    const itemsReadUrl = `${apiUrl}/items/123/read/${readId}`;
    const res = await fetch(itemsReadUrl);
    const itemsReadResults: ItemReadResults = await res.json();

    if (res.ok) {
      return itemsReadResults;
    }
  } catch(err) {
    console.log(err);
  }
};
