import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import { Snippet } from "~/src/types/ResearchAssistant";
import ChevronIcon from "./ChevronIcon";
import SnippetList from "./SnippetList";

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
          <Text fontWeight="medium" marginTop="m">
            Relevant sections
          </Text>
          <SnippetList snippets={snippets} workId={workId} />
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
