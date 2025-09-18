import { Box } from "@nypl/design-system-react-components";
import React from "react";
import { ApiWork } from "~/src/types/WorkQuery";
import Link from "../Link/Link";

const ViewEditionsLink: React.FC<{ work: ApiWork }> = ({ work }) => {
  const editionCount = work.edition_count;
  const previewEdition = work.editions && work.editions[0];

  return (
    editionCount > 1 && (
      <Box marginTop="xs">
        <Link
          to={{
            pathname: `work/${work.uuid}`,
            query: { showAll: true, featured: previewEdition.edition_id },
            hash: "#all-editions",
          }}
          linkVariant="standalone"
        >
          {`View All ${editionCount} Editions`}
        </Link>
      </Box>
    )
  );
};

export default ViewEditionsLink;
