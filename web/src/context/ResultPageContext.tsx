import React, { createContext, useContext } from "react";
import { PageType } from "../types/ResearchAssistant";

type ResultPageContextType = {
    onReadOnline: (linkId: number) => void;
    page: PageType;
};

const ResultPageContext = createContext<ResultPageContextType>({
    onReadOnline: () => { },
    page: "keyword",
});

export const ResultPageProvider: React.FC<{
    children?: React.ReactNode;
    value: ResultPageContextType;
}> = ({ children, value }) => {
    return (
        <ResultPageContext.Provider value={value}>
            {children}
        </ResultPageContext.Provider>
    );
};

export function useResultPageContext() {
    const context = useContext(ResultPageContext);
    if (context === null) {
        throw new Error(
            "useResultPageContext must be used within a ResultPageProvider"
        );
    }
    return context;
}
