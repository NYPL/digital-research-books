// Dectector for seeing if using Blink engine (Chrome-based browser)
export const isBlinkClient = (): boolean => {
  if (typeof navigator === "undefined") return false;
  const ua = navigator.userAgent || "";
  const uaData = (navigator as any).userAgentData;
  const brands: string =
    uaData?.brands?.map((b: { brand: string }) => b.brand).join(" ") || "";
  if (/iPhone|iPad|iPod/i.test(ua)) return false;
  return (
    /Chromium|Google Chrome|Microsoft Edge|Opera|Brave/i.test(brands) ||
    /(Chrome|Chromium|Edg|OPR|Brave|Arc)/i.test(ua)
  );
};

const replaceCombiningHalfMarks = (text: string | undefined): string => {
  if (!text) return text || "";
  // This replaces the two half marks and split-half-mark patterns with a single tie bar.
  return text
    .replace(/\ufe20\ufe21/g, "\u0361")
    .replace(/(.)\ufe20(.)\ufe21/g, "$1\u0361$2")
    .replace(/(.)\ufe20(.)(.)\ufe21/g, "$1\u0361$2\u0361$3");
};

export const normalizeCombiningHalfMarksDeep = <T>(value: T): T => {
  if (typeof value === "string") {
    return replaceCombiningHalfMarks(value) as T;
  }

  if (Array.isArray(value)) {
    return value.map((item) => normalizeCombiningHalfMarksDeep(item)) as T;
  }

  if (value && typeof value === "object") {
    const normalized = Object.entries(value as Record<string, unknown>).reduce(
      (acc, [key, objValue]) => {
        acc[key] = normalizeCombiningHalfMarksDeep(objValue);
        return acc;
      },
      {} as Record<string, unknown>
    );

    return normalized as T;
  }

  return value;
};
