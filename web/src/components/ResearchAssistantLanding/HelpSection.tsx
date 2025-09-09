import { Box, Button, Text } from "@nypl/design-system-react-components";
import Link from "../Link/Link";

const HelpSection: React.FC = () => {
    return (
        <Box backgroundColor="ui.bg.default" paddingX="xs">
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
                <Text
                    fontSize="2rem"
                    fontWeight="semibold"
                    color="section.research.secondary"
                    marginBottom="l"
                >
                    <>
                        Have a question? Get <Link to="#help">help</Link> or{" "}
                        <Link to="#learn-more">learn more about this project</Link>
                    </>
                </Text>
                <Button
                    id="try-it-button"
                    buttonType="secondary"
                    borderColor="section.research.secondary"
                    color="section.research.secondary"
                    margin="0 auto"
                    borderRadius="2px"
                    fontWeight="medium"
                    onClick={() => {
                        const heroSectionElement = document.getElementById("hero-section");
                        if (heroSectionElement) {
                            heroSectionElement.scrollIntoView({ behavior: "smooth" });
                        }
                    }}
                >
                    Try it out ↑
                </Button>
            </Box>
        </Box>
    );
};

export default HelpSection;
