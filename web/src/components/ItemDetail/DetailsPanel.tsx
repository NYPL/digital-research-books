import { Box, Text, VStack } from "@nypl/design-system-react-components";
import AuthorsList from "../AuthorsList/AuthorsList";
import Link from "../Link/Link";
import { ApiWork } from "~/src/types/WorkQuery";

interface DetailsPanelProps {
    work: ApiWork;
}

const DetailsPanel: React.FC<DetailsPanelProps> = ({ work }) => (
    <VStack alignItems="left" gap="xs">
      <Box>
        <Text fontWeight="bold">Authors</Text>
        <AuthorsList authors={work.authors} />
      </Box>
      <Box>
        <Text fontWeight="bold">Subjects</Text>
        {work.subjects && work.subjects.length > 0 ? (
          <VStack alignItems="left" gap="xxs">
            {work.subjects
              .filter((subject) => subject.heading)
              .map((subject, i) => (
                <Link
                  key={`subject-link-${i}`}
                  to={{
                    pathname: "/keyword-search",
                    query: { query: `subject:${subject.heading}` },
                  }}
                  isUnderlined={false}
                >
                  {subject.heading}
                </Link>
              ))}
          </VStack>
        ) : (
          <Text>Unknown subjects</Text>
        )}
      </Box>
      <Box>
        <Text fontWeight="bold">Languages</Text>
        <Text>{work.languages?.join(", ") || "Unknown languages"}</Text>
      </Box>
    </VStack>
);

export default DetailsPanel;
