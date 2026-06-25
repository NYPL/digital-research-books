import {
  MINIMUM_CLEAR_DELAY,
  MS_PER_WORD,
} from "~/src/constants/chatAnnouncer";

let container: HTMLElement | null = null;

const estimateReadingTime = (text: string) => {
  const words = text.trim().split(/\s+/).length;
  return Math.max(MINIMUM_CLEAR_DELAY, words * MS_PER_WORD);
};

const createAnnouncerNode = (text: string): HTMLElement => {
  const node = document.createElement("p");
  node.textContent = text;
  return node;
};

export const chatAnnouncer = {
  mount: () => {
    if (container || typeof document === "undefined") return;

    // Guard against HMR duplicate
    const existing = document.querySelector("[data-chat-announcer]");
    if (existing) {
      container = existing as HTMLElement;
      return;
    }

    container = document.createElement("div");
    container.setAttribute("aria-live", "polite");
    container.setAttribute("aria-relevant", "additions");
    container.setAttribute("aria-atomic", "false");
    container.setAttribute("data-chat-announcer", "true");

    Object.assign(container.style, {
      position: "absolute",
      width: "1px",
      height: "1px",
      padding: "0",
      margin: "-1px",
      overflow: "hidden",
      clip: "rect(0,0,0,0)",
      whiteSpace: "nowrap",
      border: "0",
    });

    document.body.appendChild(container);
  },

  announce: (text: string) => {
    if (!container) return;

    const node = createAnnouncerNode(text);
    container.appendChild(node);

    setTimeout(() => {
      node.remove();
    }, estimateReadingTime(text));
  },

  unmount: () => {
    container?.remove();
    container = null;
  },
};
