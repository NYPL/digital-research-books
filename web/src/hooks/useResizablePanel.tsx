import { useCallback, useEffect, useRef, useState } from "react";
import { DEFAULT_MOBILE_PANEL_HEIGHT } from "~/src/constants/researchAssistant";

type UseResizablePanelOptions = {
  showChat: boolean;
  onHidePanel: () => void;
};

const KEYBOARD_RESIZE_STEP = 32;

export function useResizablePanel({
  showChat,
  onHidePanel,
}: UseResizablePanelOptions) {
  const [mobilePanelHeight, setMobilePanelHeight] = useState(
    DEFAULT_MOBILE_PANEL_HEIGHT
  );
  const currentHeightRef = useRef(DEFAULT_MOBILE_PANEL_HEIGHT);
  const resizeStateRef = useRef({
    isResizing: false,
    startY: 0,
    startHeight: DEFAULT_MOBILE_PANEL_HEIGHT,
    shouldHide: false,
  });
  const rafIdRef = useRef<number | null>(null);
  const onHidePanelRef = useRef(onHidePanel);

  useEffect(() => {
    onHidePanelRef.current = onHidePanel;
  }, [onHidePanel]);

  const applyPanelHeightCssVar = useCallback((height: number) => {
    if (typeof document === "undefined") return;
    document
      .getElementById("mainContent")
      ?.style.setProperty("--mobile-panel-height", `${height}px`);
  }, []);

  const setPanelHeight = useCallback(
    (height: number) => {
      currentHeightRef.current = height;
      applyPanelHeightCssVar(height);
      setMobilePanelHeight(height);
    },
    [applyPanelHeightCssVar]
  );

  const getMaxPanelHeight = useCallback(() => {
    if (typeof window === "undefined") return 900;
    return window.innerHeight;
  }, []);

  const clampPanelHeight = useCallback(
    (height: number) => {
      const maxPanelHeight = getMaxPanelHeight();
      return Math.min(
        Math.max(height, DEFAULT_MOBILE_PANEL_HEIGHT),
        maxPanelHeight
      );
    },
    [getMaxPanelHeight]
  );

  useEffect(() => {
    if (typeof window === "undefined") return;

    const initializeHeight = () => {
      const viewportHeight = window.innerHeight;
      const defaultHeight = Math.round(viewportHeight * 0.8);
      const baseHeight =
        currentHeightRef.current === DEFAULT_MOBILE_PANEL_HEIGHT
          ? defaultHeight
          : currentHeightRef.current;
      setPanelHeight(clampPanelHeight(baseHeight));
    };

    initializeHeight();
    window.addEventListener("resize", initializeHeight);

    return () => {
      window.removeEventListener("resize", initializeHeight);
    };
  }, [clampPanelHeight, setPanelHeight]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const handlePointerMove = (event: PointerEvent) => {
      if (!resizeStateRef.current.isResizing) return;
      event.preventDefault();

      const deltaY = resizeStateRef.current.startY - event.clientY;
      const rawHeight = resizeStateRef.current.startHeight + deltaY;

      if (rawHeight < DEFAULT_MOBILE_PANEL_HEIGHT - 30) {
        resizeStateRef.current.shouldHide = true;
        if (rafIdRef.current !== null) {
          cancelAnimationFrame(rafIdRef.current);
          rafIdRef.current = null;
        }
      } else {
        resizeStateRef.current.shouldHide = false;
        if (rafIdRef.current !== null) {
          cancelAnimationFrame(rafIdRef.current);
        }
        rafIdRef.current = requestAnimationFrame(() => {
          const nextHeight = clampPanelHeight(rawHeight);
          currentHeightRef.current = nextHeight;
          applyPanelHeightCssVar(nextHeight);
          rafIdRef.current = null;
        });
      }
    };

    const stopResizing = () => {
      if (rafIdRef.current !== null) {
        cancelAnimationFrame(rafIdRef.current);
        rafIdRef.current = null;
      }
      if (resizeStateRef.current.shouldHide) {
        onHidePanelRef.current();
      } else {
        setMobilePanelHeight(currentHeightRef.current);
      }
      resizeStateRef.current.isResizing = false;
      resizeStateRef.current.shouldHide = false;
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopResizing);
    window.addEventListener("pointercancel", stopResizing);

    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopResizing);
      window.removeEventListener("pointercancel", stopResizing);
    };
  }, [applyPanelHeightCssVar, clampPanelHeight]);

  const handleExpandToFull = useCallback(() => {
    setPanelHeight(getMaxPanelHeight());
  }, [getMaxPanelHeight, setPanelHeight]);

  const handleDecreaseToMin = useCallback(() => {
    setPanelHeight(DEFAULT_MOBILE_PANEL_HEIGHT);
  }, [setPanelHeight]);

  const handleResizeKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (!showChat) return;

      const step = event.shiftKey
        ? KEYBOARD_RESIZE_STEP * 2
        : KEYBOARD_RESIZE_STEP;

      switch (event.key) {
        case "ArrowUp":
          event.preventDefault();
          setPanelHeight(clampPanelHeight(currentHeightRef.current + step));
          break;
        case "ArrowDown":
          event.preventDefault();
          setPanelHeight(clampPanelHeight(currentHeightRef.current - step));
          break;
        default:
          break;
      }
    },
    [clampPanelHeight, setPanelHeight, showChat]
  );

  useEffect(() => {
    if (showChat) {
      setPanelHeight(DEFAULT_MOBILE_PANEL_HEIGHT);
    } else {
      resizeStateRef.current.isResizing = false;
      setPanelHeight(DEFAULT_MOBILE_PANEL_HEIGHT);
    }
  }, [setPanelHeight, showChat]);

  useEffect(() => {
    return () => {
      if (rafIdRef.current !== null) {
        cancelAnimationFrame(rafIdRef.current);
      }
    };
  }, []);

  const handleResizeStart = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      if (!showChat) return;

      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      resizeStateRef.current = {
        isResizing: true,
        startY: event.clientY,
        startHeight: currentHeightRef.current,
        shouldHide: false,
      };
    },
    [showChat]
  );

  return {
    mobilePanelHeight,
    handleResizeStart,
    handleResizeKeyDown,
    handleExpandToFull,
    handleDecreaseToMin,
    getMaxPanelHeight,
  };
}
