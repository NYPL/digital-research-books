import { Box, Radio, RadioGroup } from "@nypl/design-system-react-components";
import { ItemLink } from "~/src/types/DataModel";
import Link from "../../Link/Link";

const onDownloadOptionChange = (): void => {
  throw new Error("Function not implemented.");
};

interface DownloadOptionsPanelProps {
  authorNames: string[];
  downloadLink: ItemLink;
  title: string;
  isLoggedIn: boolean;
}

const DownloadOptionsPanel: React.FC<DownloadOptionsPanelProps> = () =>
  //     {
  //     authorNames,
  //     downloadLink,
  //     title,
  //     isLoggedIn,
  // }
  {
    return (
      <Box>
        <RadioGroup
          defaultValue="pdf"
          labelText="Format"
          onChange={onDownloadOptionChange}
          name="downloadOptionFormat"
          marginBottom="s"
          id="format-radio-group"
          sx={{ ".ds-radioGroup-stack": { gap: "xs" } }}
        >
          <Radio labelText="E-book (PDF)" value="pdf" />
          <Radio labelText="Text (.txt)" value="txt" />
          <Radio labelText="Text (.zip)" value="zip" />
        </RadioGroup>
        <RadioGroup
          defaultValue="full"
          labelText="Range"
          onChange={onDownloadOptionChange}
          name="downloadOptionRange"
          id="range-radio-group"
          sx={{ ".ds-radioGroup-stack": { gap: "xs" } }}
        >
          <Radio labelText="Entire e-book" value="full" />
          <Radio labelText="Current page" value="page" />
        </RadioGroup>
        {/* TODO: Re-add after download is implemented on the backend
            <DownloadLink
                authors={authorNames}
                downloadLink={downloadLink}
                title={title}
                isLoggedIn={isLoggedIn}
            />
            Placeholder for Download Link
            */}
        <Link
          to="#"
          variant="buttonSecondary"
          borderColor="section.research.secondary"
          color="section.research.secondary"
          marginTop="m"
          width="fit-content"
        >
          Download
        </Link>
      </Box>
    );
  };

export default DownloadOptionsPanel;
