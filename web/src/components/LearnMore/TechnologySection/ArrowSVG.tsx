import React from "react";

const ArrowSVG: React.FC = () => {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        height: "100%",
      }}
    >
      <div
        style={{
          flex: 1,
          width: "2px",
          backgroundColor: "#006166",
          minHeight: "8px",
          marginBottom: "-5px",
        }}
      />
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="15"
        height="9"
        viewBox="-1 0 17 10"
        fill="none"
        aria-hidden={true}
      >
        <path
          d="M6.65667 8.707C7.04719 9.098 7.68036 9.098 8.07088 8.707L14.4348 2.343C14.8254 1.953 14.8254 1.319 14.4348 0.929C14.0443 0.538 13.4112 0.538 13.0206 0.929L7.36378 6.586L1.70692 0.929C1.3164 0.538 0.683231 0.538 0.292707 0.929C-0.0978174 1.319 -0.0978173 1.953 0.292707 2.343L6.65667 8.707Z"
          fill="#006166"
        />
      </svg>
    </div>
  );
};

export default ArrowSVG;
