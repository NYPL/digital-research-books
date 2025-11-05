import { Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import { ApiWork } from "~/src/types/WorkQuery";
import Link from "../Link/Link";

const EditionsLinks: React.FC<{ work: ApiWork }> = ({ work }) => {

  return (
    work.editions.length > 1 && (
      <Flex flexDir="column" gap="xs">
        {work.editions.slice(1).map((edition) => (
          <Link
            key={edition.edition_id}
            to={`edition/${edition.edition_id}`}
            isUnderlined={false}
          >
            <Text size="body2">{edition.publication_date} edition</Text>
          </Link>
        ))}
      </Flex>
    )
  );
};

export default EditionsLinks;
