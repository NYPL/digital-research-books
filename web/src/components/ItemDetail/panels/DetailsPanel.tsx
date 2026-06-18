import { Box, List, Text, VStack } from "@nypl/design-system-react-components";
import { ApiItem, WorkEdition } from "~/src/types/DataModel";
import { ApiWork } from "~/src/types/WorkQuery";
import Link from "../../Link/Link";

interface DetailsPanelProps {
  previewItem: ApiItem;
  previewEdition: WorkEdition;
  publisherNames: string[];
  work: ApiWork;
}

const DetailsPanel: React.FC<DetailsPanelProps> = ({
  previewItem,
  previewEdition,
  publisherNames,
  work,
}) => (
  <VStack alignItems="left" gap="xs">
    <Box>
      <Text fontWeight="bold">Copyright</Text>
      <Link to="/copyright" isUnderlined={false}>
        {previewItem && previewItem.rights && previewItem.rights.length > 0
          ? `${previewItem.rights[0].rightsStatement}`
          : "Unknown"}
      </Link>
    </Box>
    <Box>
      <Text fontWeight="bold">Edition</Text>
      <Text>{previewEdition.publication_date || "Unknown"}</Text>
    </Box>
    <Box>
      <Text fontWeight="bold">Publisher</Text>
      <Text>{publisherNames.join(", ") || "Unknown"}</Text>
    </Box>
    <Box>
      <Text fontWeight="bold">Place of publication</Text>
      <Text>{previewEdition.publication_place || "Unknown"}</Text>
    </Box>
    <Box>
      <Text fontWeight="bold">Subjects</Text>
      {work.subjects && work.subjects.length > 0 ? (
        <List alignItems="left" gap="xxs" variant="ul" noStyling>
          {work.subjects
            .filter((subject) => subject.heading)
            .map((subject, i) => (
              <li key={`subject-link-${i}`}>
                <Link
                  to={{
                    pathname: "/keyword-search",
                    query: { query: `subject:${subject.heading}` },
                  }}
                  isUnderlined={false}
                >
                  {subject.heading}
                </Link>
              </li>
            ))}
        </List>
      ) : (
        <Text>Unknown</Text>
      )}
    </Box>
    <Box>
      <Text fontWeight="bold">Languages</Text>
      <Text>
        {work.languages && work.languages.length > 0
          ? work.languages.map((l) => l.language).join(", ")
          : "Unknown"}
      </Text>
    </Box>
  </VStack>
);

export default DetailsPanel;
