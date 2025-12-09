// ResearchAssistantPanel
const CONTENT_PADDING_VALUE = "1rem";
const OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2)";

export const MARGIN_BLEED = `calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE})`;
export const PADDING_COUNTER = `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE})`;

const MARGIN_COMPACT = "-2rem";
const PADDING_COMPACT = "l";
const MARGIN_RIGHT_COMPACT = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE}) * 2)`;
const MARGIN_RIGHT_BLEED = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${PADDING_COUNTER}) * 2)`;

export const getPanelLayout = (hasResults: boolean) => {
    return {
        marginX: hasResults ? MARGIN_COMPACT : MARGIN_BLEED,
        paddingX: hasResults ? PADDING_COMPACT : PADDING_COUNTER,
        marginRight: hasResults ? MARGIN_RIGHT_COMPACT : MARGIN_RIGHT_BLEED,
        paddingRight: hasResults ? undefined : PADDING_COUNTER,
    };
};

// ItemDetail
const GAP_SIZE = 32;
const CONTENT_TOTAL = 1280;
const NET_CONTENT = CONTENT_TOTAL - 2 * GAP_SIZE;
const NET_DISABLED = CONTENT_TOTAL - GAP_SIZE;
const ITEM_OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2 + 2rem)";

export const getGridColumns = (vraEnabled: boolean) => {
    return vraEnabled
        ? {
            base: "1fr",
            md: `
                1fr 
                ${NET_CONTENT * 0.25}px
                ${NET_CONTENT * 0.5}px
                ${NET_CONTENT * 0.25}px
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

export const getGridPaddingX = (vraEnabled: boolean) => {
    return {
        base: vraEnabled ? "1rem" : ITEM_OUTER_MARGIN_CALC,
        md: vraEnabled ? "1.5rem" : ITEM_OUTER_MARGIN_CALC,
        xl: vraEnabled ? "1rem" : ITEM_OUTER_MARGIN_CALC,
    };
};
