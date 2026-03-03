import { Box, Flex, Icon, Text } from "@nypl/design-system-react-components";
import React from "react";
import { Snippet } from "~/src/types/ResearchAssistant";
import Link from "../Link/Link";

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
        fontWeight="medium"
        marginTop={isOpen ? "m" : "s"}
        cursor="pointer"
        _hover={{
          color: "section.research.primary",
        }}
      >
        <Text>
          {isOpen ? "Hide relevant sections" : "View relevant sections"}
        </Text>
        <Icon
          name="arrow"
          size="xsmall"
          iconRotation={isOpen ? "rotate180" : "rotate0"}
          color="section.research.secondary"
          _hover={{
            color: "section.research.primary",
          }}
          decorative
        />
      </Flex>
    </Box>
  );
};

export default RelevantSections;
