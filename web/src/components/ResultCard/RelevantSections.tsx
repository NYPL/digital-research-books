import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import { Snippet } from "~/src/types/ResearchAssistant";
import Link from "../Link/Link";
import ChevronIcon from "./ChevronIcon";

interface RelevantSectionsProps {
  snippets: Snippet[];
  workId?: string;
}

const RelevantSections: React.FC<RelevantSectionsProps> = ({
  snippets,
  workId,
}) => {
  const [isOpen, setIsOpen] = React.useState(false);
  if (!snippets || snippets.length === 0) return null;

  return (
    <Box fontSize="desktop.caption">
      {isOpen && (
        <>
          <Text fontWeight="medium" marginBottom="xs" marginTop="m">
            Relevant sections
          </Text>
          <Flex flexDir="column" gap="s">
            {snippets.map((snippet, index) => (
              <Flex
                key={index}
                flexDir="column"
                gap="xs"
                borderTop="1px dotted"
                borderColor="ui.border.default"
              >
                <Link
                  to={`/item/${workId}?previewItemId=${
                    snippet.item_id
                  }&previewPage=${String(snippet.start_page).padStart(8, "0")}`}
                  marginTop="xs"
                  fontWeight="medium"
                >
                  Page {snippet.start_page}
                </Link>
                <Text isItalic>&quot;{snippet.text}&quot;</Text>
              </Flex>
            ))}
          </Flex>
        </>
      )}
      <Flex
        gap="xxs"
        alignItems="center"
        color="section.research.secondary"
        onClick={() => setIsOpen(!isOpen)}
        fontSize="desktop.body.body2"
        fontWeight="medium"
        marginTop={isOpen ? "m" : "s"}
        cursor="pointer"
        _hover={{
          color: "section.research.primary",
        }}
      >
        <Text>
          {isOpen
            ? "Hide relevant sections"
            : `View ${snippets.length} relevant sections`}
        </Text>
        <ChevronIcon
          iconRotation={isOpen ? "rotate(0deg)" : "rotate(180deg)"}
        />
      </Flex>
    </Box>
  );
};

export default RelevantSections;
