import { Box, Text, VStack } from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import { ApiItem, WorkEdition } from "~/src/types/DataModel";

interface AboutItemPanelProps {
    previewItem: ApiItem;
    previewEdition: WorkEdition;
    publisherNames: string[];
}

const AboutItemPanel: React.FC<AboutItemPanelProps> = ({
    previewItem,
    previewEdition,
    publisherNames,
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
            <Text>{previewEdition.publication_date || "Unknown date"}</Text>
        </Box>
        <Box>
            <Text fontWeight="bold">Publisher</Text>
            <Text>{publisherNames.join(", ") || "Publisher unknown"}</Text>
        </Box>
        <Box>
            <Text fontWeight="bold">Place of publication</Text>
            <Text>
                {previewEdition.publication_place || "Place of publication unknown"}
            </Text>
        </Box>
    </VStack>
);

export default AboutItemPanel;
