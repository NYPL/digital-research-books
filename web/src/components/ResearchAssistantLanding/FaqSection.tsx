import { Accordion, Flex, Heading } from "@nypl/design-system-react-components";
import { ACCORDION_SECTIONS } from "~/src/constants/faqData";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
    >
      <Heading
        level="h2"
        size="heading2"
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom="xxl"
      >
        Frequently asked questions
      </Heading>
      <Flex flexDir="column" gap="l">
        {ACCORDION_SECTIONS.map((section, index) => (
          <Flex key={index} gap="l">
            <Heading
              level="h3"
              size="heading3"
              fontFamily="Domine"
              width="20%"
              textAlign="left"
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
