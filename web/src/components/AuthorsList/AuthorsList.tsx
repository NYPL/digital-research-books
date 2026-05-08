import React from "react";
import Link from "~/src/components/Link/Link";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import { Agent } from "~/src/types/DataModel";
import { ApiSearchQuery } from "~/src/types/SearchQuery";

const AuthorsList: React.FC<{ authors: Agent[] }> = ({ authors }) => {
  if (!authors || authors.length === 0) return <>Unknown</>;

  return (
    <>
      {authors.map((author: Agent, i: number) => {
        const authorLinkText = author.name;
        const query: ApiSearchQuery = {
          query: author.viaf ? `viaf:${author.viaf}` : `author:${author.name}`,
        };
        if (author.viaf) {
          query.display = `author:${author.name}`;
        }
        return (
          <React.Fragment
            key={
              author.viaf ? `author-${author.viaf}` : `author-${author.name}`
            }
          >
            <Link
              to={{
                pathname: "/search",
                query: query,
              }}
              onClick={() => {
                // GTM Tagging: sidebar_source_click, author
                trackEvent({
                  event: "sidebar_source_click",
                  interaction: "Click",
                  location: "Results",
                  metadata_field: "Author",
                });
              }}
            >
              {authorLinkText}
            </Link>
            {i < authors.length - 1 && ", "}
          </React.Fragment>
        );
      })}
    </>
  );
};

export default AuthorsList;
