import { chatAnnouncer } from "~/src/lib/chatAnnouncer/ChatAnnouncer";

export function useChatAnnouncer() {
  return { announce: chatAnnouncer.announce };
}
