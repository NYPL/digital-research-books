import {
  Box,
  Flex,
  SkeletonLoader,
  VStack,
} from "@nypl/design-system-react-components";
import React from "react";
import {
  HEADER_HEIGHT,
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import ResultsBanner from "../ResultsBanner";

export const CatalogResultsSkeleton: React.FC = () => {
  const accordionContentWidths = ["40%", "30%", "15%"];

  return (
    <Flex
      flexDir="column"
      bgColor="ui.bg.default"
      gap="s"
      paddingX={{ base: "s", md: "l" }}
    >
      <Box
        bgColor="ui.bg.default"
        borderBottom="1px solid"
        borderColor="ui.border.default"
        marginX="-2rem"
        paddingX="l"
        paddingY="s"
        marginLeft={MARGIN_BLEED}
        height={HEADER_HEIGHT}
        paddingLeft={PADDING_COUNTER}
      >
        <SkeletonLoader
          showContent={false}
          showImage={false}
          sx={{ div: { margin: 0 } }}
        />
      </Box>
      <ResultsBanner />
      {[1, 2].map((index) => (
        <Box
          key={index}
          border="1px solid"
          borderColor="ui.border.default"
          padding="s"
          backgroundColor="ui.white"
          borderTop="2px solid"
          borderTopColor="ui.gray.medium"
          fontSize="desktop.body.body2"
        >
          <VStack align="left" spacing="s">
            <Box>
              <SkeletonLoader
                showButton
                showContent={false}
                showHeading={false}
                showImage={false}
                maxWidth="120px"
                margin={0}
                sx={{
                  div: { borderRadius: "4px", height: "27px" },
                }}
              />
              <SkeletonLoader
                showImage={false}
                headingSize={1}
                contentSize={2}
                sx={{
                  div: { borderRadius: "4px" },
                }}
              />
            </Box>
            <Box border="1px solid #E5E5E5" borderRadius="4px">
              {accordionContentWidths.map((accordionWidth, index) => (
                <Flex
                  key={index}
                  height="40px"
                  padding="s"
                  alignItems="center"
                  justifyContent="space-between"
                  borderBottom={index < 2 ? "1px solid #E5E5E5" : "none"}
                >
                  <Box width="100%">
                    <SkeletonLoader
                      showImage={false}
                      showHeading={false}
                      contentSize={1}
                      margin={0}
                      width={accordionWidth}
                      sx={{
                        div: { borderRadius: "4px", margin: 0, width: "100%" },
                      }}
                    />
                  </Box>
                  <Box>
                    <SkeletonLoader
                      layout="row"
                      showHeading={false}
                      showImage={false}
                      contentSize={1}
                      width="24px"
                      margin={0}
                      sx={{
                        div: { width: "100%", height: "24px" },
                      }}
                    />
                  </Box>
                </Flex>
              ))}
            </Box>
            <Flex gap="xs" height="40px">
              {[1, 2].map((index) => (
                <SkeletonLoader
                  key={index}
                  layout="row"
                  showButton
                  showContent={false}
                  showHeading={false}
                  showImage={false}
                  maxWidth="160px"
                  margin={0}
                  sx={{
                    div: { borderRadius: "4px" },
                  }}
                />
              ))}
            </Flex>
          </VStack>
        </Box>
      ))}
    </Flex>
  );
};

export default CatalogResultsSkeleton;
