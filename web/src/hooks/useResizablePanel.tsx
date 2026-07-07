import { useCallback, useEffect, useRef, useState } from "react";
import { DEFAULT_MOBILE_PANEL_HEIGHT } from "~/src/constants/researchAssistant";

type UseResizablePanelOptions = {
  showChat: boolean;
  onHidePanel: () => void;
};

export function useResizablePanel({
  showChat,
  onHidePanel,
}: UseResizablePanelOptions) {
  const [mobilePanelHeight, setMobilePanelHeight] = useState(
    DEFAULT_MOBILE_PANEL_HEIGHT
  );
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
      setMobilePanelHeight((previousHeight) => {
        if (previousHeight === 640) {
          return clampPanelHeight(defaultHeight);
        }
        return clampPanelHeight(previousHeight);
      });
    };

    initializeHeight();
    window.addEventListener("resize", initializeHeight);

    return () => {
      window.removeEventListener("resize", initializeHeight);
    };
  }, [clampPanelHeight]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const handlePointerMove = (event: PointerEvent) => {
      if (!resizeStateRef.current.isResizing) return;
      event.preventDefault();

      const deltaY = resizeStateRef.current.startY - event.clientY;
      const rawHeight = resizeStateRef.current.startHeight + deltaY;

      if (rawHeight < DEFAULT_MOBILE_PANEL_HEIGHT - 30) {
        resizeStateRef.current.shouldHide = true;
      } else {
        resizeStateRef.current.shouldHide = false;
        if (rafIdRef.current !== null) {
          cancelAnimationFrame(rafIdRef.current);
        }
        rafIdRef.current = requestAnimationFrame(() => {
          setMobilePanelHeight(clampPanelHeight(rawHeight));
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
  }, [clampPanelHeight]);

  const handleExpandToFull = useCallback(() => {
    setMobilePanelHeight(getMaxPanelHeight());
  }, [getMaxPanelHeight]);

  const handleDecreaseToMin = useCallback(() => {
    setMobilePanelHeight(DEFAULT_MOBILE_PANEL_HEIGHT);
  }, []);

  useEffect(() => {
    if (showChat) {
      setMobilePanelHeight(512);
    } else {
      resizeStateRef.current.isResizing = false;
      setMobilePanelHeight(512);
    }
  }, [showChat]);

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
        startHeight: mobilePanelHeight,
        shouldHide: false,
      };
    },
    [mobilePanelHeight, showChat]
  );

  return {
    mobilePanelHeight,
    handleResizeStart,
    handleExpandToFull,
    handleDecreaseToMin,
    getMaxPanelHeight,
  };
}
