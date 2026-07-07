import { act, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { DEFAULT_MOBILE_PANEL_HEIGHT } from "~/src/constants/researchAssistant";
import { useResizablePanel } from "./useResizablePanel";

type HookHarnessProps = {
  showChat?: boolean;
  onHidePanel?: () => void;
};

const HookHarness: React.FC<HookHarnessProps> = ({
  showChat = true,
  onHidePanel = () => {},
}) => {
  const { mobilePanelHeight, handleResizeStart } = useResizablePanel({
    showChat,
    onHidePanel,
  });

  return (
    <>
      <div data-testid="height">{mobilePanelHeight}</div>
      <div data-testid="resize-handle" onPointerDown={handleResizeStart} />
    </>
  );
};

const getHeight = () => {
  const value = screen.getByTestId("height").textContent;
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    throw new Error(`Expected numeric panel height, got: ${String(value)}`);
  }

  return parsed;
};

describe("useResizablePanel", () => {
  let originalRequestAnimationFrame: typeof window.requestAnimationFrame;
  let originalCancelAnimationFrame: typeof window.cancelAnimationFrame;

  beforeAll(() => {
    if (!("PointerEvent" in window)) {
      Object.defineProperty(window, "PointerEvent", {
        value: MouseEvent,
        configurable: true,
        writable: true,
      });
    }

    if (!HTMLElement.prototype.setPointerCapture) {
      Object.defineProperty(HTMLElement.prototype, "setPointerCapture", {
        value: jest.fn(),
        writable: true,
      });
    }

    originalRequestAnimationFrame = window.requestAnimationFrame;
    originalCancelAnimationFrame = window.cancelAnimationFrame;

    window.requestAnimationFrame = (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    };

    window.cancelAnimationFrame = jest.fn();
  });

  afterAll(() => {
    window.requestAnimationFrame = originalRequestAnimationFrame;
    window.cancelAnimationFrame = originalCancelAnimationFrame;
  });

  it("clamps drag height to the maximum", () => {
    Object.defineProperty(window, "innerHeight", {
      value: 700,
      configurable: true,
      writable: true,
    });

    render(<HookHarness />);

    const handle = screen.getByTestId("resize-handle");

    act(() => {
      fireEvent.pointerDown(handle, { clientY: 300, pointerId: 1 });
      fireEvent.pointerMove(window, { clientY: -100 });
      fireEvent.pointerUp(window);
    });

    expect(getHeight()).toBe(700);
  });

  it("clamps drag height to the minimum", () => {
    Object.defineProperty(window, "innerHeight", {
      value: 1200,
      configurable: true,
      writable: true,
    });

    render(<HookHarness />);

    const handle = screen.getByTestId("resize-handle");

    act(() => {
      fireEvent.pointerDown(handle, { clientY: 300, pointerId: 1 });
      fireEvent.pointerMove(window, { clientY: 320 });
      fireEvent.pointerUp(window);
    });

    expect(getHeight()).toBe(DEFAULT_MOBILE_PANEL_HEIGHT);
  });

  it("updates panel height by drag", () => {
    Object.defineProperty(window, "innerHeight", {
      value: 1200,
      configurable: true,
      writable: true,
    });

    render(<HookHarness />);

    const handle = screen.getByTestId("resize-handle");

    act(() => {
      fireEvent.pointerDown(handle, { clientY: 300, pointerId: 1 });
      fireEvent.pointerMove(window, { clientY: 200 });
      fireEvent.pointerUp(window);
    });

    expect(getHeight()).toBe(DEFAULT_MOBILE_PANEL_HEIGHT + 100);
  });

  it("hides the drawer when dragged below the minimum threshold", () => {
    Object.defineProperty(window, "innerHeight", {
      value: 1200,
      configurable: true,
      writable: true,
    });

    const onHidePanel = jest.fn();

    render(<HookHarness onHidePanel={onHidePanel} />);

    const handle = screen.getByTestId("resize-handle");

    act(() => {
      fireEvent.pointerDown(handle, { clientY: 300, pointerId: 1 });
      fireEvent.pointerMove(window, { clientY: 370 });
      fireEvent.pointerUp(window);
    });

    expect(onHidePanel).toHaveBeenCalledTimes(1);
  });
});
