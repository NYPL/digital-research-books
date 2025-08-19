import {
    Box,
    Button,
    Heading,
    Icon,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";

const HelpSection: React.FC = () => {
    return (
        <Box backgroundColor="ui.bg.default">
            <Box
                paddingY="xxl"
                color="section.research.secondary"
                textAlign="center"
                margin="0 auto"
                maxWidth="1280px"
                width="100%"
                display="flex"
                flexDir="column"
                alignItems="center"
            >
                <Heading
                    level="h2"
                    size="heading3"
                    fontWeight="semibold"
                    color="section.research.secondary"
                    marginBottom="l"
                >
                    <>
                        Have a question? Get <Link to="#help">help</Link> or{" "}
                        <Link to="#learn-more">learn more about this project</Link>
                    </>
                </Heading>
                <Button
                    id="try-it-button"
                    buttonType="secondary"
                    borderColor="section.research.secondary"
                    color="section.research.secondary"
                >
                    Try it out <Icon />
                </Button>
            </Box>
        </Box>
    );
};

export default HelpSection;
