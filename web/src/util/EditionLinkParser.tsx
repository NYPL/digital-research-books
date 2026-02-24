import React from "react";

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
      <a
        key={`edition-${editionId}-${match.index}`}
        href={`#edition-${editionId}`}
        onClick={(e) => {
          e.preventDefault();
          onEditionClick(editionId);
        }}
        style={{
          color: "var(--nypl-colors-ui-link-primary)",
          textDecoration: "underline",
          cursor: "pointer",
        }}
      >
        {editionText}
      </a>
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
    element.style.backgroundColor = "var(--nypl-colors-section-research-primary-05)";
    
    setTimeout(() => {
      element.style.backgroundColor = originalBg;
    }, 2000);
  }
};