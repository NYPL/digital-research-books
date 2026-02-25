import { render } from "~/src/__tests__/testUtils/render";
import ResultPublisherAndLocation from "~/src/components/ResultCard/ResultPublisherAndLocation";

test("renders nothing when no publisher or location", () => {
  const { container } = render(
    <ResultPublisherAndLocation pubPlace={""} publishers={[]} />
  );
  expect(container).toBeEmptyDOMElement();
});