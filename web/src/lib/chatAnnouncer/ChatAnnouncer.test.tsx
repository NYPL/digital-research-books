import { MINIMUM_CLEAR_DELAY } from "~/src/constants/chatAnnouncer";
import { chatAnnouncer } from "./ChatAnnouncer";

beforeAll(() => {
  global.requestAnimationFrame = (cb: FrameRequestCallback) => {
    cb(0);
    return 0;
  };
});

beforeEach(() => {
  chatAnnouncer.mount();
});

afterEach(() => {
  chatAnnouncer.unmount();
  jest.useRealTimers();
});

describe("mount", () => {
  test("appends an announcer element to document.body", () => {
    const el = document.querySelector("[aria-live='polite']");
    expect(el).not.toBeNull();
    expect(document.body.contains(el)).toBe(true);
  });

  test("sets the correct ARIA attributes", () => {
    const el = document.querySelector("[aria-live='polite']")!;
    expect(el).toHaveAttribute("aria-live", "polite");
    expect(el).toHaveAttribute("aria-relevant", "additions");
  });

  test("applies sr-only styles", () => {
    const el = document.querySelector("[aria-live='polite']") as HTMLElement;
    expect(el).toHaveStyle({ position: "absolute" });
    expect(el).toHaveStyle({ width: "1px" });
    expect(el).toHaveStyle({ height: "1px" });
    expect(el).toHaveStyle({ overflow: "hidden" });
  });

  test("calling mount twice does not create a second element", () => {
    chatAnnouncer.mount();
    const els = document.querySelectorAll("[aria-live='polite']");
    expect(els.length).toBe(1);
  });

  test("reuses an existing container node if present (HMR guard)", () => {
    const existing = document.querySelector("[data-chat-announcer]");
    chatAnnouncer.unmount();
    document.body.appendChild(existing!);

    chatAnnouncer.mount();
    const els = document.querySelectorAll("[data-chat-announcer]");
    expect(els.length).toBe(1);
  });
});

describe("unmount", () => {
  test("removes the announcer element from the DOM", () => {
    chatAnnouncer.unmount();
    const el = document.querySelector("[aria-live='polite']");
    expect(el).toBeNull();
  });

  test("does not throw if called before mount", () => {
    chatAnnouncer.unmount();
    expect(() => chatAnnouncer.unmount()).not.toThrow();
  });

  test("allows a clean remount after unmounting", () => {
    chatAnnouncer.unmount();
    chatAnnouncer.mount();
    const el = document.querySelector("[data-chat-announcer]");
    expect(el).not.toBeNull();
  });
});

describe("announce", () => {
  test("appends a child node with the announced text", () => {
    chatAnnouncer.announce("Test");
    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children.length).toBe(1);
    expect(container.children[0]).toHaveTextContent("Test");
  });

  test("appends a child node for each announcement", () => {
    chatAnnouncer.announce("First");
    chatAnnouncer.announce("Second");

    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children.length).toBe(2);
    expect(container.children[0]).toHaveTextContent("First");
    expect(container.children[1]).toHaveTextContent("Second");
  });

  test("does not overwrite a message currently being announced", () => {
    chatAnnouncer.announce("User message");
    chatAnnouncer.announce("Loading response");
    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children[0]).toHaveTextContent("User message");
    expect(container.children[1]).toHaveTextContent("Loading response");
  });

  test("does nothing if mount has not been called", () => {
    chatAnnouncer.unmount();
    expect(() => chatAnnouncer.announce("test")).not.toThrow();
  });

  test("removes a node after the estimated reading time", () => {
    jest.useFakeTimers();

    chatAnnouncer.announce("Test");
    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children.length).toBe(1);

    jest.runAllTimers();
    expect(container.children.length).toBe(0);
  });

  test("removes each node independently without affecting others", () => {
    jest.useFakeTimers();

    chatAnnouncer.announce("word ".repeat(40).trim());
    chatAnnouncer.announce("Test");

    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children.length).toBe(2);

    jest.advanceTimersByTime(MINIMUM_CLEAR_DELAY + 1);
    expect(container.children.length).toBe(1);
    expect(container.children[0]).toHaveTextContent(/word/);

    jest.runAllTimers();
    expect(container.children.length).toBe(0);
  });

  test("can re-announce the same text after it has been removed", () => {
    jest.useFakeTimers();

    chatAnnouncer.announce("Same message");
    jest.runAllTimers();

    chatAnnouncer.announce("Same message");
    const container = document.querySelector("[data-chat-announcer]")!;
    expect(container.children[0]).toHaveTextContent("Same message");
  });
});

describe("reading time estimation", () => {
  test("uses a minimum delay of 500ms for very short messages", () => {
    jest.useFakeTimers();

    chatAnnouncer.announce("Test");
    const el = document.querySelector("[aria-live='polite']")!;

    jest.advanceTimersByTime(MINIMUM_CLEAR_DELAY - 1);
    expect(el).toHaveTextContent("Test");

    jest.advanceTimersByTime(1);
    expect(el).toBeEmptyDOMElement();
  });

  test("holds longer messages for longer than the minimum", () => {
    jest.useFakeTimers();

    const longMessage = "word ".repeat(30).trim();
    chatAnnouncer.announce(longMessage);

    const el = document.querySelector("[aria-live='polite']")!;

    jest.advanceTimersByTime(MINIMUM_CLEAR_DELAY);
    expect(el).toHaveTextContent(longMessage);

    jest.runAllTimers();
    expect(el).toBeEmptyDOMElement();
  });
});
