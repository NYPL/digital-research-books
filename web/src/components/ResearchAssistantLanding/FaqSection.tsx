import {
    Accordion,
    AccordionTypes,
    Box,
    Heading,
    Text,
} from "@nypl/design-system-react-components";

const FaqSection: React.FC = () => {
    const accordionData = [
        {
            accordionType: "default" as AccordionTypes,
            label:
                "What types of content does the Virtual Research Assistant search over?",
            panel: (
                <Box>
                    <Text>
                        The Virtual Research Assistant is currently in beta and searches
                        over 1 million scholarly e-books from our Research Collections to
                        formulate its responses. This includes public domain and open access
                        materials, as well as digitized versions of in-copyright research
                        books owned by NYPL.
                    </Text>

                    <Text noSpace>
                        In the future, the Virtual Research Assistant will expand to search
                        millions of additional scholarly e-books from our collection. It
                        will also search physical books from our Research Catalog, as well
                        as journals and databases we subscribe to. Our mission is for it to
                        become a singular and comprehensive entry point into the entire
                        Research Collections.
                    </Text>
                </Box>
            ),
        },
        {
            accordionType: "default" as AccordionTypes,
            label:
                "How accurate are the answers provided by the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
        {
            accordionType: "default" as AccordionTypes,
            label:
                "Will my prompts be used to train the Virtual Research Assistant's model?",
            panel: <Box></Box>,
        },
        {
            accordionType: "default" as AccordionTypes,
            label:
                "Will my information and data remain private and secure when I use the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
        {
            accordionType: "default" as AccordionTypes,
            label:
                "Where can I can learn more about this project and the New York Public Library's use of AI?",
            panel: <Box></Box>,
        },
        {
            accordionType: "default" as AccordionTypes,
            label:
                "Do I need an NYPL library card to use the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
    ];

    return (
        <Box backgroundColor="section.research.primary">
            <Box
                paddingY="xxl"
                textAlign="left"
                margin="0 auto"
                maxWidth="1280px"
                width="100%"
            >
                <Heading
                    level="h2"
                    size="heading3"
                    fontWeight="semibold"
                    color="ui.white"
                    marginBottom="l"
                    textAlign="center"
                >
                    Frequently asked questions
                </Heading>
                <Accordion
                    backgroundColor="ui.white"
                    id="faq-accordion"
                    accordionData={accordionData}
                />
            </Box>
        </Box>
    );
};

export default FaqSection;
