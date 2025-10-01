import { Button, Text } from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import { forwardRef } from "react";
import SectionContainer from "./SectionContainer";

interface HelpSectionProps {
    heroSectionRef: React.RefObject<HTMLDivElement>;
}

const HelpSection: React.ForwardRefExoticComponent<
    HelpSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HelpSectionProps>(({ heroSectionRef }, ref) => {
    return (
        <SectionContainer
            backgroundColor="ui.bg.default"
            color="section.research.secondary"
            display="flex"
            flexDir="column"
            alignItems="center"
            ref={ref}
        >
            <Text
                fontSize="2rem"
                fontWeight="semibold"
                color="section.research.secondary"
                marginBottom="l"
            >
                <>
                    Have a question? Get{" "}
                    <Link
                        to="https://www.nypl.org/get-help/contact-us"
                        color="section.research.secondary"
                        isUnderlined
                    >
                        help
                    </Link>{" "}
                    or{" "}
                    <Link
                        to="#learn-more"
                        color="section.research.secondary"
                        isUnderlined
                    >
                        learn more about this project
                    </Link>
                </>
            </Text>
            <Button
                id="try-it-button"
                variant="secondary"
                borderColor="section.research.secondary"
                color="section.research.secondary"
                margin="0 auto"
                borderRadius="2px"
                fontWeight="medium"
                onClick={() =>
                    heroSectionRef.current?.scrollIntoView({ behavior: "smooth" })
                }
            >
                Try it out ↑
            </Button>
        </SectionContainer>
    );
});

HelpSection.displayName = "HelpSection";

export default HelpSection;
