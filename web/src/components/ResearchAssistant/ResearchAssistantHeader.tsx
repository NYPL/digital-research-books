import { Box } from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import {
  getPanelLayout,
  HEADER_HEIGHT,
  ITEM_PAGE_PADDING_RIGHT,
} from "~/src/constants/researchAssistant";

type ResearchAssistantHeaderProps = {
  children: React.ReactNode;
  showChat: boolean;
};

const ResearchAssistantHeader: React.FC<ResearchAssistantHeaderProps> = ({
  children,
  showChat,
}) => {
  const router = useRouter();
  const isItemPage = router.pathname.startsWith("/item/");
  const { marginX, marginRight, paddingX, paddingRight } = getPanelLayout();
  return (
    <Box
      bgColor="section.research.primary"
      display="flex"
      justifyContent="space-between"
      alignItems="center"
      borderBottom="1px white solid"
      borderRadius={{ base: "8px 8px 0 0", md: "0" }}
      marginLeft={marginX}
      marginRight={marginRight}
      paddingLeft={{
        base: paddingX.base,
        md: showChat ? paddingX.md : "s",
      }}
      // itemPage conditional is here for now, this will be removed with item page responsive PR
      paddingRight={{
        base: paddingRight.base,
        md: showChat
          ? isItemPage
            ? ITEM_PAGE_PADDING_RIGHT
            : paddingRight.md
          : "0",
      }}
      position="sticky"
      paddingY="s"
      top="0"
      zIndex="999"
      height={HEADER_HEIGHT}
    >
      {children}
    </Box>
  );
};

export default ResearchAssistantHeader;
