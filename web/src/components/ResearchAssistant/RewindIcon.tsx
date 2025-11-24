import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const RewindIcon: React.FC = () => {
    return (
        <Icon size="medium" color="transparent">
            <svg
                xmlns="http://www.w3.org/2000/svg"
                width="17"
                height="16"
                viewBox="0 0 17 16"
                fill="none"
            >
                <path
                    d="M1 6.25C1 6.25 2.50374 4.20116 3.72538 2.97868C4.94702 1.7562 6.6352 1 8.5 1C12.2279 1 15.25 4.02208 15.25 7.75C15.25 11.4779 12.2279 14.5 8.5 14.5C5.42268 14.5 2.82633 12.4407 2.01382 9.625M1 6.25V1.75M1 6.25H5.5"
                    stroke="white"
                    stroke-width="2"
                    stroke-linecap="round"
                    stroke-linejoin="round"
                />
            </svg>
        </Icon>
    );
};

export default RewindIcon;
