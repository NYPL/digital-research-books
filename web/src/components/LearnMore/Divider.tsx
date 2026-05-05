interface DividerProps {
  orientation: "vertical" | "horizontal";
  color: string;
}

const Divider = ({ orientation, color }: DividerProps) => {
  return (
    <div
      style={{
        borderStyle: "dashed",
        borderColor: color,
        borderWidth: "1px",
        width: orientation === "vertical" ? "1px" : "auto",
        height: orientation === "vertical" ? "6rem" : "1px",
        alignSelf: orientation === "vertical" ? "stretch" : undefined,
      }}
    ></div>
  );
};

export default Divider;
