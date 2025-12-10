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
}
