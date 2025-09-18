import {
    Accordion,
    Box,
    Heading,
    Text,
} from "@nypl/design-system-react-components";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
    const accordionData = [
        {
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
            label:
                "How accurate are the answers provided by the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
        {
            label:
                "Will my prompts be used to train the Virtual Research Assistant's model?",
            panel: <Box></Box>,
        },
        {
            label:
                "Will my information and data remain private and secure when I use the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
        {
            label:
                "Where can I can learn more about this project and the New York Public Library's use of AI?",
            panel: <Box></Box>,
        },
        {
            label:
                "Do I need an NYPL library card to use the Virtual Research Assistant?",
            panel: <Box></Box>,
        },
    ];

    return (
        <SectionContainer backgroundColor="section.research.primary">
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
        </SectionContainer>
    );
};

export default FaqSection;
