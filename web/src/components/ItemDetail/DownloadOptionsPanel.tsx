import {
    Box,
    Radio,
    RadioGroup,
} from "@nypl/design-system-react-components";
import DownloadLink from "../EditionCard/DownloadLink";
import { ItemLink } from "~/src/types/DataModel";

const onDownloadOptionChange = (): void => {
    throw new Error("Function not implemented.");
};

interface DownloadOptionsPanelProps {
    authorNames: string[];
    downloadLink: ItemLink;
    title: string;
    isLoggedIn: boolean;
}

const DownloadOptionsPanel: React.FC<DownloadOptionsPanelProps> = ({
    authorNames,
    downloadLink,
    title,
    isLoggedIn,
}) => {
    return (
        <Box>
            <RadioGroup
                defaultValue="full"
                labelText="Range"
                onChange={onDownloadOptionChange}
                name="downloadOptionRange"
            >
                <Radio labelText="Entire e-book" value="full" />
                <Radio labelText="Current page" value="page" />
            </RadioGroup>
            <DownloadLink
                authors={authorNames}
                downloadLink={downloadLink}
                title={title}
                isLoggedIn={isLoggedIn}
            />
        </Box>
    );
};

export default DownloadOptionsPanel;
