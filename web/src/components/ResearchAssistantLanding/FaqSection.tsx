import { Accordion, Flex, Heading } from "@nypl/design-system-react-components";
import { ACCORDION_SECTIONS } from "~/src/constants/faqData";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      paddingX={{ base: "none", md: "s" }}
    >
      <Heading
        level="h2"
        fontSize={{
          base: "mobile.heading.heading3",
          md: "desktop.heading.heading2",
        }}
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom={{ base: "s", md: "xxl" }}
      >
        Frequently asked questions
      </Heading>
      <Flex flexDir="column" gap={{ base: "s", md: "l" }}>
        {ACCORDION_SECTIONS.map((section, index) => (
          <Flex
            key={index}
            gap={{ base: "s", md: "l" }}
            flexDir={{ base: "column", md: "row" }}
          >
            <Heading
              level="h3"
              fontSize={{
                base: "mobile.heading.heading4",
                md: "desktop.heading.heading3",
              }}
              fontFamily="Domine"
              width={{ base: "100%", md: "20%" }}
              textAlign={{ base: "center", md: "left" }}
            >
              {section.title}
            </Heading>
            <Accordion
              backgroundColor="ui.white"
              color="ui.black"
              flex="1"
              textAlign="left"
              id={`faq-accordion-${index}`}
              accordionData={section.data}
              sx={{
                button: {
                  fontWeight: "bold",
                },
                "button:focus": {
                  outlineColor: "section.research.secondary",
                },
                "button[aria-expanded=true]": {
                  bgColor: "section.research.secondary",
                  color: "ui.white",
                },
                "button[aria-expanded=true]:hover": {
                  bgColor: "section.research.primary",
                },
                "button[aria-expanded=false]": {
                  bgColor: "ui.white",
                  color: "section.research.secondary",
                },
                "button[aria-expanded=false]:hover": {
                  bgColor: "section.research.primary-10",
                },
                ".chakra-collapse": {
                  bgColor: "ui.white",
                },
              }}
            />
          </Flex>
        ))}
      </Flex>
    </SectionContainer>
  );
};

export default FaqSection;
