import React, { createContext, useContext } from "react";

type PageType = "keyword" | "researchAssistant";

type ResultPageContextType = {
    onPreview: (url: string) => void;
    onReadOnline: (linkId: number) => void;
    page: PageType;
};

const ResultPageContext = createContext<ResultPageContextType>({
    onPreview: () => { },
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
