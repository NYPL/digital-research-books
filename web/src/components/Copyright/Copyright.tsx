/* eslint-disable max-len */
import React from "react";
import {
  Box,
  Heading,
  List,
  Template,
  TemplateBreakout,
  TemplateContent,
  TemplateMain,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

const Copyright: React.FC = () => {
  const breakoutElement = (
    <DrbBreakout breadcrumbsData={[{ url: "/copyright", text: "Copyright" }]} />
  );
  const contentElement = (
    <Box paddingBottom="l">
      <Heading level="h1" marginBottom="s">Copyright Explanations</Heading>
      <Heading level="h2" marginBottom="s">Public Domain</Heading>
      <p>
        Works in the public domain have no copyright (in most cases because the
        copyright term has expired) and you are free to use, adapt, share, and
        distribute the work in any way you wish.
      </p>
      <Heading level="h2" marginBottom="s">Public Domain (US Only)</Heading>
      <p>
        Works may be in the public domain in the Unites States (where you can
        use and distribute them without restriction), but still subject to
        copyright laws and restrictions outside the United States.
      </p>
      <Heading level="h2" marginBottom="s">Creative Commons Licenses</Heading>
      <p>
        <Link to="https://creativecommons.org/">Creative Commons licenses</Link>{" "}
        allow rights-holders to grant people the right to freely use their
        creations in certain ways and perhaps by meeting certain requirements.
        There are several licenses with different combinations of four basic
        clauses:
      </p>
      <List variant="dl">
        <dt>BY</dt>

        <dd>
          Attribution — You must give appropriate credit, provide a link to the
          license, and indicate if changes were made. You may do so in any
          reasonable manner, but not in any way that suggests the licensor
          endorses you or your use.
        </dd>

        <dt>NC</dt>

        <dd>
          NonCommercial — You may not use the material for commercial purposes.{" "}
        </dd>

        <dt>ND</dt>

        <dd>
          NoDerivatives — If you remix, transform, or build upon the material,
          you may not distribute the modified material.
        </dd>

        <dt>SA</dt>

        <dd>
          ShareAlike — If you remix, transform, or build upon the material, you
          must distribute your contributions under the same license as the
          original.
        </dd>
      </List>
      <p>
        The following Creative Commons licenses are applied to materials in
        Digital Research Books Beta. Refer to linked license descriptions for
        specifics:
      </p>
      <List variant="ul" marginBottom="s">
        <li>
          <Link to="https://creativecommons.org/licenses/by/3.0/">
            Attribution 3.0 Unported (CC BY 3.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by/4.0/">
            Attribution 4.0 International (CC BY 4.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-sa/3.0/">
            Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-sa/4.0/">
            Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nd/3.0/">
            Attribution-NoDerivs 3.0 Unported (CC BY-ND 3.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nd/4.0/">
            Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nc/4.0/">
            Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nc-sa/3.0/">
            Attribution-NonCommercial-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nc-sa/4.0/">
            Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA
            4.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nc-nd/3.0/">
            Attribution-NonCommercial-NoDerivs 3.0 Unported (CC BY-NC-ND 3.0)
          </Link>
        </li>

        <li>
          <Link to="https://creativecommons.org/licenses/by-nc-nd/4.0/">
            Attribution-NonCommercial-NoDerivatives 4.0 International (CC
            BY-NC-ND 4.0)
          </Link>
        </li>
      </List>
      <Heading level="h2" marginBottom="s">CC0 Public Domain Dedication</Heading>
      <p>
        The{" "}
        <Link to="https://creativecommons.org/publicdomain/zero/1.0/">
          CC0 1.0 Universal (CC0 1.0) Public Domain Dedication
        </Link>{" "}
        waives all rights under worldwide copyright law. It allows you to use,
        adapt, share, and distribute the work as if it were in the Public
        domain.
      </p>
      <Heading level="h2" marginBottom="s">GNU General Public License</Heading>
      <p>
        For a work licensed under the{" "}
        <Link to="http://www.gnu.org/licenses/gpl.html">
          GNU General Public License
        </Link>
        , you may copy, distribute and modify the work as long as any
        modifications are also made available under the GPL.
      </p>
      <Heading level="h2" marginBottom="s">In Copyright</Heading>
      <p>
        Works that are In Copyright are protected by copyright and/or related
        rights. You are free to use this Item in any way that is permitted by
        the copyright and related rights legislation that applies to your use.
        For other uses you need to obtain permission from the rights-holder(s).
      </p>
    </Box>
  );
  return (
    <Template variant="narrow">
      <TemplateBreakout>{breakoutElement}</TemplateBreakout>
      <TemplateMain>
        <TemplateContent>{contentElement}</TemplateContent>
      </TemplateMain>
    </Template>
  );
};

export default Copyright;
