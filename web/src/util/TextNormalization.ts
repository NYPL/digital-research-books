const replaceCombiningHalfMarks = (text: string | undefined): string => {
  if (!text) return text || "";
  // This replaces the two half marks and split-half-mark patterns with a single tie bar.
  return text
    .replace(/\ufe20\ufe21/g, "\u0361")
    .replace(/(.)\ufe20(.)\ufe21/g, "$1\u0361$2");
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
