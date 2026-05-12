import { Heading, List, Text } from "@nypl/design-system-react-components";
import React from "react";
import ReactMarkdown from "react-markdown";
import Link from "~/src/components/Link/Link";

export const renderMarkdownContent = (
  markdownText: string,
  onEditionClick: (editionId: string) => void
): React.ReactNode => {
  // Convert edition tags to markdown links: $1 = edition id, $2 = edition text
  const processedText = markdownText.replace(
    /<edition id="([^"]+)">([^<]+)<\/edition>/g,
    "[$2](#edition-$1)"
  );

  return (
    <ReactMarkdown
      components={{
        p: ({ node, children }) => {
          const isFirstBlock =
            node?.position?.start?.line === 1 ||
            node?.position?.start?.offset === 0;

          if (isFirstBlock) {
            return <Text as="span">{children}</Text>;
          }

          return <Text marginTop="xs">{children}</Text>;
        },

        h1: ({ children }) => (
          <Heading level="h1" size="heading6" fontWeight="bold" marginTop="xs">
            {children}
          </Heading>
        ),

        h2: ({ children }) => (
          <Heading level="h2" size="heading7" fontWeight="bold" marginTop="xs">
            {children}
          </Heading>
        ),

        h3: ({ children }) => (
          <Heading level="h3" size="heading8" fontWeight="bold" marginTop="xs">
            {children}
          </Heading>
        ),

        ul: ({ children }) => (
          <List variant="ul" marginTop="xs">
            {children}
          </List>
        ),

        ol: ({ children }) => (
          <List variant="ol" marginTop="xs">
            {children}
          </List>
        ),

        a: ({ href, children }) => {
          if (href?.startsWith("#edition-")) {
            const id = href.replace("#edition-", "");
            return (
              <Link
                to={href}
                onClick={(e) => {
                  e.preventDefault();
                  onEditionClick(id);
                }}
              >
                {children}
              </Link>
            );
          }

          // Only render text if it's not an edition link
          return <>{children}</>;
        },
      }}
    >
      {processedText}
    </ReactMarkdown>
  );
};
