import React from "react";
import {
  Box,
  Heading,
  TemplateAppContainer,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import appConfig from "~/config/appConfig";

const About: React.FC = () => {
  const apiEnv = process.env["APP_ENV"];
  const workUrl =
    appConfig.aboutPageWork[apiEnv] ||
    "/work/5950e6df-9d99-42fe-8924-1116166a2acb";

  const breakoutElement = (
    <DrbBreakout breadcrumbsData={[{ url: "/about", text: "About" }]} />
  );

  const contentPrimaryElement = (
    <Box paddingBottom="l">
      <Heading level="h1">
        <span>
          <span className="rn-section-title__emphasis">
            Digital Research Books
          </span>{" "}
          Beta
        </span>
      </Heading>

      <p>
        Digital Research Books Beta is an experimental project, now in early
        Beta testing, that collects digital versions of research books from many
        different sources, including Open Access publications, into one
        convenient place to search.
      </p>

      <p>
        All the materials in Digital Research Books Beta are completely free to
        read and most of them you can download and keep, with no library card
        required. The books are either in the{" "}
        <Link to="/copyright">public domain</Link>, with no restrictions on your
        use of them, or under{" "}
        <Link to="/copyright">Creative Commons licences</Link> that may have
        some conditions, but only on redistribution or adaptation.
      </p>

      <p>
        In addition to collecting these digital editions, we group all the
        editions of the same title together as a single “work.” For instance
        there are many editions of{" "}
        <Link to={workUrl}>
          Mary Wollstonecraft’s A Vindication of the Rights of Woman
        </Link>
        , many of them available digitally. We group them all together under a
        single search result and try to make the differences between them--years
        when and places where they were published, for instance--easy to
        understand.
      </p>

      <Heading
        level="h2"
        id="sources-and-data-heading"
        text="Sources and Data"
      />

      <p>
        The material in Digital Research Books Beta are drawn from several
        public sources, mainly{" "}
        <Link to="https://www.hathitrust.org/">HathiTrust</Link>,{" "}
        <Link to="https://doabooks.org/">
          The Directory of Open Access Books
        </Link>
        , and <Link to="http://www.gutenberg.org/">Project Gutenberg</Link>. We
        are continuously adding more books from these and other sources. We then
        cross-reference them with library records from NYPL and{" "}
        <Link to="https://www.worldcat.org/">WorldCat</Link>, using OCLC’s
        experimental{" "}
        <Link to="http://classify.oclc.org/classify2/">Classify</Link> to make
        connections between different editions of the same work.
      </p>

      <Heading
        level="h2"
        id="beta-testing-heading"
        text="What does Beta Testing mean?"
      />

      <p>
        It means that this is a work-in-progress. We are constantly working on
        the interface and trying new ideas to deal with the data. If you visit
        repeatedly, sometimes it may look different. If you search for the same
        thing often, the results may change--hopefully for the better as we
        refine how the search works. There may be errors. The cross-referencing
        is automated and based on millions of library records including some
        that are inaccurate. Part of what we are exploring is how to detect and
        adjust mistakes, and how to make sure we don’t introduce any new ones.
      </p>

      <p>
        It also means that the project may change radically. We may change the
        URL. We may learn that a different approach is necessary. We may learn
        that it isn’t useful enough to anyone to continue. If you find books in
        Digital Research Books Beta that are especially useful to you, you
        should download a copy so that you have one no matter what becomes of
        this project.
      </p>

      <p>
        Most of all, it means your feedback is important! Most pages have a
        feedback button in the bottom right corner. We want to know what you
        think. If there are things you like or dislike, if there’s a feature
        missing, if you find an error please tell us in the feedback!
      </p>
    </Box>
  );
  return (
    <TemplateAppContainer
      breakout={breakoutElement}
      contentPrimary={contentPrimaryElement}
    />
  );
};

export default About;
