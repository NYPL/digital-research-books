import { Text } from "@nypl/design-system-react-components";

interface NumberCircleProps {
  number: number;
}

const NumberCircle = ({ number }: NumberCircleProps) => {
  return (
    <Text
      width="2rem"
      height="2rem"
      borderRadius="50%"
      backgroundColor="section.research.secondary"
      color="#fff"
      display="flex"
      alignItems="center"
      justifyContent="center"
      fontWeight="700"
      fontSize="1.125rem"
      margin="0 auto"
      marginBottom="xs"
    >
      {number}
    </Text>
  );
};

export default NumberCircle;
