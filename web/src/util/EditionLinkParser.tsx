import React from "react";
import Link from "../components/Link/Link";

export const EXPLICIT_EDITION_PAGE_REGEX = /\(edition_(\d+),\s*page\s+(\d+)\)/gi;
export const IMPLICIT_PAGE_REGEX = /\(page\s+(\d+)\)/gi;

const EDITION_IN_TEXT_REGEX = /<edition id="(\d+)">[^<]+<\/edition>|edition_(\d+)/gi;

const formatPreviewPage = (page: string): string => page.padStart(8, "0");

export const getLastEditionIdBeforeIndex = (
  text: string,
  endIndex: number,
  fallbackEditionId?: string
): string | undefined => {
  const beforeText = text.slice(0, endIndex);
  EDITION_IN_TEXT_REGEX.lastIndex = 0;

  let match: RegExpExecArray | null;
  let latestEditionId: string | undefined;
  while ((match = EDITION_IN_TEXT_REGEX.exec(beforeText)) !== null) {
    latestEditionId = match[1] || match[2];
  }

  return latestEditionId || fallbackEditionId;
};

export const getLastReferencedEditionId = (
  text: string,
  fallbackEditionId?: string
): string | undefined => {
  return getLastEditionIdBeforeIndex(text, text.length, fallbackEditionId);
};

export const getItemPageLink = (
  page: string,
  workId: string,
  itemId: string
) => {
  return {
    pathname: `/item/${workId}`,
    query: {
      previewItemId: itemId,
      previewPage: formatPreviewPage(page),
    },
  };
};

export const parseEditionLinks = (
  text: string,
  onEditionClick: (editionId: string) => void
): React.ReactNode => {
  const editionRegex = /<edition id="(\d+)">([^<]+)<\/edition>/g;
  const parts: React.ReactNode[] = [];
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = editionRegex.exec(text)) !== null) {
    // Add text before the match
    if (match.index > lastIndex) {
      parts.push(text.substring(lastIndex, match.index));
    }

    // Add the clickable edition link
    const editionId = match[1];
    const editionText = match[2];
    parts.push(
      <Link
        key={`edition-${editionId}-${match.index}`}
        to={`#edition-${editionId}`}
        onClick={(e) => {
          e.preventDefault();
          onEditionClick(editionId);
        }}
      >
        {editionText}
      </Link>
    );

    lastIndex = match.index + match[0].length;
  }

  // Add remaining text after the last match
  if (lastIndex < text.length) {
    parts.push(text.substring(lastIndex));
  }

  return parts.length > 0 ? parts : text;
};

export const scrollToEdition = (editionId: string) => {
  const element = document.getElementById(`edition-${editionId}`);
  if (element) {
    element.scrollIntoView({
      behavior: "smooth",
      block: "center",
    });

    // Optional: Add a highlight effect
    element.style.transition = "background-color 0.3s ease";
    const originalBg = element.style.backgroundColor;
    element.style.backgroundColor =
      "var(--nypl-colors-section-research-primary-05)";

    setTimeout(() => {
      element.style.backgroundColor = originalBg;
    }, 2000);
  }
};
