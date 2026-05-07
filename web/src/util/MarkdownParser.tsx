import { Heading, List, Text } from "@nypl/design-system-react-components";
import React from "react";
import Link from "~/src/components/Link/Link";
import {
  EXPLICIT_EDITION_PAGE_REGEX,
  getItemPageLink,
  getLastEditionIdBeforeIndex,
  getLastReferencedEditionId,
  IMPLICIT_PAGE_REGEX,
} from "~/src/util/EditionLinkParser";

type ListType = "ol" | "ul";

interface MarkdownBlock {
  type: "paragraph" | "list" | "heading";
  content?: string;
  listType?: ListType;
  items?: string[];
  language?: string;
}

const HEADING_REGEX = /^(#{1,6})\s+(.+)$/;
const UNORDERED_LIST_REGEX = /^\s*[-+*]\s+(.+)$/;
const ORDERED_LIST_REGEX = /^\s*\d+\.\s+(.+)$/;

const BOLD_REGEX = /\*\*(.+?)\*\*|__(.+?)__/g;
const ITALIC_REGEX = /(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)|_(.+?)_/g;
const EDITION_REGEX = /<edition id="(\d+)">([^<]+)<\/edition>/g;

const flushParagraphBlock = (
  paragraphLines: string[],
  blocks: MarkdownBlock[]
): string[] => {
  if (paragraphLines.length > 0) {
    blocks.push({
      type: "paragraph",
      content: paragraphLines.join(" "),
    });
  }
  return [];
};

const flushListBlock = (
  currentList: { type: ListType; items: string[] } | null,
  blocks: MarkdownBlock[]
): { type: ListType; items: string[] } | null => {
  if (currentList && currentList.items.length > 0) {
    blocks.push({
      type: "list",
      listType: currentList.type,
      items: currentList.items,
    });
  }
  return null;
};

const shouldContinueCurrentList = (
  currentList: { type: ListType; items: string[] } | null,
  lines: string[],
  lineIndex: number
): boolean => {
  if (!currentList) return false;

  const nextNonEmptyLine = lines
    .slice(lineIndex + 1)
    .find((nextLine) => nextLine.trim().length > 0);

  const nextTrimmed = nextNonEmptyLine?.trim();
  const nextIsOrdered = !!nextTrimmed?.match(ORDERED_LIST_REGEX);
  const nextIsUnordered = !!nextTrimmed?.match(UNORDERED_LIST_REGEX);

  return (
    (currentList.type === "ol" && nextIsOrdered) ||
    (currentList.type === "ul" && nextIsUnordered)
  );
};

const parseBlocks = (markdownText: string): MarkdownBlock[] => {
  const normalized = markdownText.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  const blocks: MarkdownBlock[] = [];

  let paragraphLines: string[] = [];
  let currentList: { type: ListType; items: string[] } | null = null;

  lines.forEach((line, lineIndex) => {
    const trimmed = line.trim();

    if (!trimmed) {
      if (!shouldContinueCurrentList(currentList, lines, lineIndex)) {
        paragraphLines = flushParagraphBlock(paragraphLines, blocks);
        currentList = flushListBlock(currentList, blocks);
      }
      return;
    }

    const headingMatch = trimmed.match(HEADING_REGEX);
    if (headingMatch) {
      paragraphLines = flushParagraphBlock(paragraphLines, blocks);
      currentList = flushListBlock(currentList, blocks);
      blocks.push({
        type: "heading",
        content: headingMatch[2],
      });
      return;
    }

    const orderedMatch = trimmed.match(ORDERED_LIST_REGEX);
    if (orderedMatch) {
      paragraphLines = flushParagraphBlock(paragraphLines, blocks);
      if (!currentList || currentList.type !== "ol") {
        currentList = flushListBlock(currentList, blocks);
        currentList = { type: "ol", items: [] };
      }
      currentList.items.push(orderedMatch[1]);
      return;
    }

    const unorderedMatch = trimmed.match(UNORDERED_LIST_REGEX);
    if (unorderedMatch) {
      paragraphLines = flushParagraphBlock(paragraphLines, blocks);
      if (!currentList || currentList.type !== "ul") {
        currentList = flushListBlock(currentList, blocks);
        currentList = { type: "ul", items: [] };
      }
      currentList.items.push(unorderedMatch[1]);
      return;
    }

    if (currentList && currentList.items.length > 0) {
      const lastItemIndex = currentList.items.length - 1;
      currentList.items[
        lastItemIndex
      ] = `${currentList.items[lastItemIndex]} ${trimmed}`;
      return;
    }

    paragraphLines.push(trimmed);
  });

  paragraphLines = flushParagraphBlock(paragraphLines, blocks);
  currentList = flushListBlock(currentList, blocks);
  return blocks;
};

interface InlineToken {
  type: "text" | "bold" | "italic" | "edition" | "pageReference";
  content: string;
  editionId?: string;
  page?: string;
}

type ElementData =
  | { type: "edition"; data: { id: string; text: string } }
  | { type: "bold"; data: { content: string } }
  | { type: "italic"; data: { content: string } }
  | {
      type: "pageReference";
      data: { editionId: string; page: string; content: string };
    };

type ElementInfo = { start: number; end: number } & ElementData;

const addElementToList = (
  start: number,
  end: number,
  elementData: ElementData,
  elements: ElementInfo[]
): boolean => {
  const contained: ElementInfo[] = [];
  for (const elem of elements) {
    const overlaps = start < elem.end && end > elem.start;
    if (!overlaps) continue;

    const newContainsExisting = start <= elem.start && end >= elem.end;
    const existingContainsNew = elem.start <= start && elem.end >= end;

    if (existingContainsNew) {
      return false;
    }
    if (!newContainsExisting) {
      return false;
    }
    contained.push(elem);
  }

  for (const elem of contained) {
    const idx = elements.indexOf(elem);
    if (idx !== -1) elements.splice(idx, 1);
  }
  elements.push({ start, end, ...elementData });
  return true;
};

const parseInlineTokens = (
  text: string,
  fallbackEditionId?: string
): InlineToken[] => {
  const elements: ElementInfo[] = [];

  EDITION_REGEX.lastIndex = 0;
  let match;
  while ((match = EDITION_REGEX.exec(text)) !== null) {
    addElementToList(
      match.index,
      match.index + match[0].length,
      {
        type: "edition",
        data: { id: match[1], text: match[2] },
      },
      elements
    );
  }

  BOLD_REGEX.lastIndex = 0;
  while ((match = BOLD_REGEX.exec(text)) !== null) {
    addElementToList(
      match.index,
      match.index + match[0].length,
      {
        type: "bold",
        data: { content: match[1] || match[2] },
      },
      elements
    );
  }

  ITALIC_REGEX.lastIndex = 0;
  while ((match = ITALIC_REGEX.exec(text)) !== null) {
    addElementToList(
      match.index,
      match.index + match[0].length,
      {
        type: "italic",
        data: { content: match[1] || match[2] },
      },
      elements
    );
  }

  EXPLICIT_EDITION_PAGE_REGEX.lastIndex = 0;
  while ((match = EXPLICIT_EDITION_PAGE_REGEX.exec(text)) !== null) {
    addElementToList(
      match.index,
      match.index + match[0].length,
      {
        type: "pageReference",
        data: {
          editionId: match[1],
          page: match[2],
          content: match[0],
        },
      },
      elements
    );
  }

  IMPLICIT_PAGE_REGEX.lastIndex = 0;
  while ((match = IMPLICIT_PAGE_REGEX.exec(text)) !== null) {
    const relatedEditionId = getLastEditionIdBeforeIndex(
      text,
      match.index,
      fallbackEditionId
    );

    if (!relatedEditionId) continue;

    addElementToList(
      match.index,
      match.index + match[0].length,
      {
        type: "pageReference",
        data: {
          editionId: relatedEditionId,
          page: match[1],
          content: match[0],
        },
      },
      elements
    );
  }

  elements.sort((a, b) => a.start - b.start);

  const tokens: InlineToken[] = [];
  let lastIndex = 0;

  for (const elem of elements) {
    if (elem.start > lastIndex) {
      tokens.push({
        type: "text",
        content: text.substring(lastIndex, elem.start),
      });
    }

    switch (elem.type) {
      case "bold":
        tokens.push({ type: "bold", content: elem.data.content });
        break;
      case "italic":
        tokens.push({ type: "italic", content: elem.data.content });
        break;
      case "edition":
        tokens.push({
          type: "edition",
          content: elem.data.text,
          editionId: elem.data.id,
        });
        break;
      case "pageReference":
        tokens.push({
          type: "pageReference",
          content: elem.data.content,
          editionId: elem.data.editionId,
          page: elem.data.page,
        });
        break;
    }

    lastIndex = elem.end;
  }

  if (lastIndex < text.length) {
    tokens.push({
      type: "text",
      content: text.substring(lastIndex),
    });
  }

  return tokens;
};

type EditionLinkData = { workId?: string; itemId?: string };

const renderBoldToken = (
  content: string,
  key: string,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  return (
    <Text key={key} as="span" fontWeight="bold">
      {renderInlineTokens(
        parseInlineTokens(content, fallbackEditionId),
        onEditionClick,
        `${key}-inner`,
        fallbackEditionId,
        getWorkAndItemIds
      )}
    </Text>
  );
};

const renderItalicToken = (
  content: string,
  key: string,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  return (
    <Text key={key} as="span" fontStyle="italic">
      {renderInlineTokens(
        parseInlineTokens(content, fallbackEditionId),
        onEditionClick,
        `${key}-inner`,
        fallbackEditionId,
        getWorkAndItemIds
      )}
    </Text>
  );
};

const renderEditionToken = (
  editionId: string,
  content: string,
  key: string,
  onEditionClick: (editionId: string) => void
): React.ReactNode => {
  return (
    <Link
      key={key}
      to={`#edition-${editionId}`}
      onClick={(event) => {
        event.preventDefault();
        onEditionClick(editionId || "");
      }}
    >
      {content}
    </Link>
  );
};

const renderInlineTokens = (
  tokens: InlineToken[],
  onEditionClick: (editionId: string) => void,
  keyPrefix: string,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode[] => {
  return tokens.map((token, index) => {
    const key = `${keyPrefix}-${token.type}-${index}`;

    switch (token.type) {
      case "text":
        return token.content;

      case "bold":
        return renderBoldToken(
          token.content,
          key,
          onEditionClick,
          fallbackEditionId,
          getWorkAndItemIds
        );

      case "italic":
        return renderItalicToken(
          token.content,
          key,
          onEditionClick,
          fallbackEditionId,
          getWorkAndItemIds
        );

      case "edition":
        return renderEditionToken(
          token.editionId || "",
          token.content,
          key,
          onEditionClick
        );

      case "pageReference": {
        const resolved = getWorkAndItemIds?.(token.editionId || "");
        return (
          <Link
            key={key}
            to={getItemPageLink(
              token.page || "",
              resolved?.workId,
              resolved?.itemId
            )}
          >
            {token.content}
          </Link>
        );
      }

      default:
        return token.content;
    }
  });
};

const renderHeadingBlock = (
  block: MarkdownBlock,
  blockIndex: number,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  if (block.type !== "heading" || !block.content) return null;
  const tokens = parseInlineTokens(block.content, fallbackEditionId);
  return (
    <Heading
      level="h3"
      size="heading8"
      key={`heading-${blockIndex}`}
      fontWeight="bold"
      marginTop="s"
    >
      {renderInlineTokens(
        tokens,
        onEditionClick,
        `heading-${blockIndex}`,
        fallbackEditionId,
        getWorkAndItemIds
      )}
    </Heading>
  );
};

const renderListItem = (
  item: string,
  blockIndex: number,
  itemIndex: number,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  const tokens = parseInlineTokens(item, fallbackEditionId);
  return (
    <li key={`list-item-${blockIndex}-${itemIndex}`}>
      {renderInlineTokens(
        tokens,
        onEditionClick,
        `list-${blockIndex}-${itemIndex}`,
        fallbackEditionId,
        getWorkAndItemIds
      )}
    </li>
  );
};

const renderListBlock = (
  block: MarkdownBlock,
  blockIndex: number,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  if (block.type !== "list" || !block.items || !block.listType) return null;

  return (
    <List key={`list-${blockIndex}`} variant={block.listType} marginTop="s">
      {block.items.map((item, itemIndex) =>
        renderListItem(
          item,
          blockIndex,
          itemIndex,
          onEditionClick,
          fallbackEditionId,
          getWorkAndItemIds
        )
      )}
    </List>
  );
};

const renderParagraphBlock = (
  block: MarkdownBlock,
  blockIndex: number,
  onEditionClick: (editionId: string) => void,
  fallbackEditionId?: string,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  const isFirstBlock = blockIndex === 0;
  const tokens = parseInlineTokens(block.content || "", fallbackEditionId);

  return (
    <Text
      key={`paragraph-${blockIndex}`}
      as={isFirstBlock ? "span" : undefined}
      marginTop={isFirstBlock ? undefined : "s"}
    >
      {renderInlineTokens(
        tokens,
        onEditionClick,
        `paragraph-${blockIndex}`,
        fallbackEditionId,
        getWorkAndItemIds
      )}
    </Text>
  );
};

export const renderMarkdownContent = (
  markdownText: string,
  onEditionClick: (editionId: string) => void,
  getWorkAndItemIds?: (editionId: string) => EditionLinkData
): React.ReactNode => {
  const blocks = parseBlocks(markdownText);
  let lastReferencedEditionId: string | undefined;

  return blocks.map((block, blockIndex) => {
    const blockContent = block.content || block.items?.join(" ") || "";

    const blockNode = (() => {
      if (block.type === "heading") {
        return renderHeadingBlock(
          block,
          blockIndex,
          onEditionClick,
          lastReferencedEditionId,
          getWorkAndItemIds
        );
      }

      if (block.type === "list") {
        return renderListBlock(
          block,
          blockIndex,
          onEditionClick,
          lastReferencedEditionId,
          getWorkAndItemIds
        );
      }

      return renderParagraphBlock(
        block,
        blockIndex,
        onEditionClick,
        lastReferencedEditionId,
        getWorkAndItemIds
      );
    })();

    lastReferencedEditionId = getLastReferencedEditionId(
      blockContent,
      lastReferencedEditionId
    );

    return blockNode;
  });
};
