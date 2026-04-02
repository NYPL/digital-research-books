import { List } from "@nypl/design-system-react-components";
import React from "react";
import { Agent } from "~/src/types/DataModel";
import { ApiEdition } from "~/src/types/EditionQuery";

// Publisher
const getPublishersList = (publishers: Agent[]): JSX.Element => {
  if (!publishers || publishers.length === 0) {
    return (
      <React.Fragment key="unavailable">Publisher Unavailable</React.Fragment>
    );
  }
  return (
    <List variant="ul" noStyling>
      {publishers.map((publisher: Agent) => {
        return <li key={publisher.name}>{publisher.name}</li>;
      })}
    </List>
  );
};

export const EditionDetailDefinitionList: React.FC<{ edition: ApiEdition }> = ({
  edition,
}) => {
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
      }}
    >
      <>
        <dt>Publication Date</dt>
        <dd>
          {edition.publication_date ? edition.publication_date : "Unknown Date"}
        </dd>

        <dt>Publication Place</dt>
        <dd>
          {edition.publication_place
            ? edition.publication_place
            : "Unknown Place"}
        </dd>

        <dt>Publisher(s)</dt>
        <dd>{getPublishersList(edition.publishers)}</dd>

        {edition.edition_statement && (
          <>
            <dt>Edition Statement</dt>
            <dd>{edition.edition_statement}</dd>
          </>
        )}
        {edition.languages && edition.languages.length > 0 && (
          <>
            <dt>Languages</dt>
            <dd>
              <List variant="ul" noStyling>
                {edition.languages.map((lang) => {
                  return (
                    <li key={`language-${lang.language}`}>{lang.language}</li>
                  );
                })}
              </List>
            </dd>
          </>
        )}
        {edition.table_of_contents && (
          <>
            <dt>Table of Contents</dt>
            <dd>{edition.table_of_contents}</dd>
          </>
        )}
        {edition.extent && (
          <>
            <dt>Extent</dt>
            <dd>{edition.extent}</dd>
          </>
        )}
        {edition.volume && (
          <>
            <dt>Volume</dt>
            <dd>{edition.volume}</dd>
          </>
        )}
        {edition.summary && (
          <>
            <dt>Summary</dt>
            <dd>{edition.summary}</dd>
          </>
        )}
      </>
    </List>
  );
};

export default EditionDetailDefinitionList;
