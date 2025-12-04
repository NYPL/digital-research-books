const CONTENT_PADDING_VALUE = "1rem";
const OUTER_MARGIN_CALC = "calc((100vw - 1280px) / 2)";

export const MARGIN_BLEED = `calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE})`;
export const PADDING_COUNTER = `calc(${OUTER_MARGIN_CALC} + ${CONTENT_PADDING_VALUE})`;

const MARGIN_COMPACT = "-2rem";
const PADDING_COMPACT = "l";
const MARGIN_RIGHT_COMPACT = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${CONTENT_PADDING_VALUE}) * 2)`;
const MARGIN_RIGHT_BLEED = `calc(calc(${OUTER_MARGIN_CALC} * -1 - ${PADDING_COUNTER}) * 2)`;

export function getPanelLayout(hasResults: boolean) {
    return {
        marginX: hasResults ? MARGIN_COMPACT : MARGIN_BLEED,
        paddingX: hasResults ? PADDING_COMPACT : PADDING_COUNTER,
        marginRight: hasResults ? MARGIN_RIGHT_COMPACT : MARGIN_RIGHT_BLEED,
        paddingRight: hasResults ? undefined : PADDING_COUNTER,
    };
}
