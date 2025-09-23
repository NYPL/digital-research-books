import React from "react";
import { List } from "@nypl/design-system-react-components";
import Link from "~/src/components/Link/Link";
import { unique, flattenDeep, uniqueAndSortByFrequency } from "~/src/util/Util";
import { Language, Subject, WorkEdition } from "~/src/types/DataModel";
import { ApiWork } from "~/src/types/WorkQuery";
import AuthorsList from "../AuthorsList/AuthorsList";

// extract unique language array from instances of a work item
const getLanguagesForWork = (work: ApiWork) =>
  work &&
  work.editions &&
  uniqueAndSortByFrequency(
    flattenDeep(
      work.editions.map(
        (edition: WorkEdition) =>
          edition.languages &&
          edition.languages.length &&
          edition.languages.map(
            (language: Language) => language && language.language
          )
      )
    )
  );

const WorkDetailDefinitionList: React.FC<{ work: ApiWork }> = ({ work }) => {
  const languages = getLanguagesForWork(work);
  return (
    <List
      title="Details"
      variant="dl"
      id="details-list"
      sx={{
        h2: {
          fontSize: "desktop.heading.heading5",
          fontWeight: "heading.heading5",
        },
        "dd, dt": {
          fontSize: "desktop.body.body2",
        },
        a: {
          textDecoration: "none",
        },
      }}
    >
      <>
        {work.alt_titles && work.alt_titles.length > 0 && (
          <>
            <dt>Alternative Titles</dt>
            <dd>
              <List variant="ul" noStyling>
                {work.alt_titles.map((title: string, i: number) => (
                  <li key={`alt-title-${i}`}>
                    <Link
                      to={{
                        pathname: "/search",
                        query: { query: `title:${title}` },
                      }}
                      isUnderlined={false}
                    >
                      {title}
                    </Link>
                  </li>
                ))}
              </List>
            </dd>
          </>
        )}
        {work.series && (
          <>
            <dt>Series</dt>
            <dd>
              {work.series}
              {work.series_position && ` ${work.series_position}`}
            </dd>
          </>
        )}
        <dt>Authors</dt>
        <dd>
          <AuthorsList authors={work.authors} />
        </dd>
        {work.subjects && work.subjects.length > 0 && (
          <>
            <dt>Subjects</dt>
            <dd>
              <List variant="ul" noStyling>
                {unique(work.subjects, "heading")
                  .sort((a: Subject, b: Subject) =>
                    a.heading &&
                    b.heading &&
                    a.heading.toLowerCase() < b.heading.toLowerCase()
                      ? -1
                      : 1
                  )
                  .map((subject: Subject, i: number) => (
                    <li key={`subject${i.toString()}`}>
                      <Link
                        to={{
                          pathname: "/search",
                          query: {
                            query: `subject:${subject.heading}`,
                          },
                        }}
                      >
                        {subject.heading}
                      </Link>
                    </li>
                  ))}
              </List>
            </dd>
          </>
        )}
        {languages && languages.length > 0 && (
          <>
            <dt>Languages</dt>
            <dd>
              <List variant="ul" noStyling>
                {languages.map((language, i) => (
                  <li key={`language${i.toString()}`}>{language}</li>
                ))}
              </List>
            </dd>
          </>
        )}
      </>
    </List>
  );
};

export default WorkDetailDefinitionList;
