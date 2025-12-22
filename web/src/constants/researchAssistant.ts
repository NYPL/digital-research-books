// ResearchAssistantPanel
const CONTENT_PADDING_VALUE = "1rem";
const OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2)";

export const MARGIN_BLEED = `calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE})`;
export const PADDING_COUNTER = `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE})`;

const MARGIN_COMPACT = "-2rem";
const PADDING_COMPACT = "l";
const MARGIN_RIGHT_COMPACT = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE}) * 2)`;

export function getPanelLayout() {
    return {
        marginX: MARGIN_COMPACT,
        paddingX: PADDING_COMPACT,
        marginRight: MARGIN_RIGHT_COMPACT,
    };
};

// ItemDetail
const GAP_SIZE = 32;
const CONTENT_TOTAL = 1280;
const NET_CONTENT = CONTENT_TOTAL - 2 * GAP_SIZE;
const NET_DISABLED = CONTENT_TOTAL - GAP_SIZE;
const ITEM_OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2 + 2rem)";
export const GRID_PADDING_X = {
    base: "1rem",
    md: "2rem",
    xl: "2rem",
};

export const getGridColumns = (vraEnabled: boolean, showChat: boolean) => {
    return vraEnabled
        ? {
            base: "1fr",
            md: showChat ? `
                1fr 
                ${NET_CONTENT * 0.25}px
                ${NET_CONTENT * 0.5}px
                ${NET_CONTENT * 0.25}px
                1fr
            ` : `
                1fr 
                ${NET_CONTENT * 0.25}px
                ${NET_CONTENT * 0.65}px
                ${NET_CONTENT * 0.1}px
                1fr
            `,
        }
        : {
            base: "1fr",
            md: `
                1fr                           
                ${NET_DISABLED * 0.3333}px  
                ${NET_DISABLED * 0.6666}px    
                1fr                            
            `,
        };
};

export const getGridRows = (backUrl?: string) => {
    return backUrl ? "auto 1fr" : "1fr";
};

export const getHeaderPaddingRight = (vraEnabled: boolean) => {
    return vraEnabled ? GRID_PADDING_X : ITEM_OUTER_MARGIN_CALC;
};
