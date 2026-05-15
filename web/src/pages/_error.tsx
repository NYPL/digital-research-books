import {
  Box,
  Button,
  Flex,
  Heading,
  Image,
  Text,
} from "@nypl/design-system-react-components";
import { NextPageContext } from "next";
import { useContext, useEffect } from "react";
import Layout from "../components/Layout/Layout";
import Link from "../components/Link/Link";
import { ERROR_LAYOUT_TEST_ID } from "../constants/testIds";
import { FeedbackContext } from "../context/FeedbackContext";

const ERROR_PERSISTS = " if the error persists.";

const errorMap = {
  500: {
    heading: "Something went wrong on our end.",
    subText: "We encountered an error while trying to load the page. ",
    tryText: "Try refreshing the page or ",
  },
  404: {
    heading: "We couldn't find that page.",
    subText:
      "The page you were looking for doesn't exist or may have moved elsewhere. ",
    tryText: "Try a different URL or ",
  },
  400: {
    heading: "There was an unexpected error.",
    subText: "We couldn't process your request at this time. ",
    tryText: "Try again later or ",
  },
};

const Error = ({ statusCode }) => {
  const { onOpen, isError, setIsError, setStatusCode } = useContext(
    FeedbackContext
  );

  useEffect(() => {
    if (!isError) {
      setIsError(true);
      setStatusCode(statusCode);
    }
  }, [isError, setIsError, setStatusCode, statusCode]);

  // default to 400 messages for unexpected error codes
  const errorValues = errorMap[statusCode]
    ? errorMap[statusCode]
    : errorMap[400];

  return (
    <Layout>
      <Flex
        alignItems="center"
        flexDir="column"
        paddingLeft="l"
        paddingRight="l"
        textAlign="center"
        role="alert"
        data-testid={ERROR_LAYOUT_TEST_ID}
      >
        <Image
          src="/images/error-img.png"
          alt=""
          marginBottom="xl"
          marginTop={{ base: "xl", md: "xxxl" }}
          width={100}
        />
        <Heading marginY="s">{errorValues.heading}</Heading>
        <Box>
          <Text display={{ base: "inline", md: "block" }}>
            {errorValues.subText}
          </Text>
          <Text display="inline">{errorValues.tryText}</Text>
          <Button
            variant="text"
            display="inline-block"
            id="contact-button"
            onClick={onOpen}
            size="large"
            padding="0"
          >
            contact us
          </Button>
          {ERROR_PERSISTS}
        </Box>
        <Box
          width="fit-content"
          marginTop={{ base: "xl", md: "xxl" }}
          marginBottom={{ base: "xxl", md: "xxxl" }}
        >
          <Link to="/" variant="buttonPrimary">
            Back to Digital Research Books
          </Link>
        </Box>
      </Flex>
    </Layout>
  );
};

Error.getInitialProps = ({ res, err }: NextPageContext) => {
  const statusCode = res ? res.statusCode : err ? err.statusCode : 404;
  return { statusCode };
};

export default Error;
