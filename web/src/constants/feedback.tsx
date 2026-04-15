import { Box, Text } from "@nypl/design-system-react-components";
import Link from "../components/Link/Link";
import { ASK_NYPL } from "./links";

export const DEFAULT_DESCRIPTION_TEXT =
  "Please share your question or feedback.";
export const THUMB_DESCIPTION_TEXT =
  "Thank you for your feedback! Would you like to provide more details?";
export const BUTTON_DESCRIPTION_TEXT =
  "Would you like to ask a question or tell us about your experience?";
export const ERROR_DESCRIPTION_TEXT = "We are here to help!";
export const ERROR_NOTIFICATION_TEXT = `You are asking for help or information about a page error.`;

export const CONFIRMATION_TEXT = (
  <Box>
    <Text marginBottom="s">
      If you asked a question and provided an email, allow us a few days to
      respond.
    </Text>
    <Text>
      To see more ways of getting in touch, visit our{" "}
      <Link to={ASK_NYPL}>ASK NYPL</Link> page.
    </Text>
  </Box>
);
