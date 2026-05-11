export const scrollToEdition = (editionId: string) => {
  const element = document.getElementById(`edition-${editionId}`);
  if (element) {
    element.scrollIntoView({
      behavior: "smooth",
      block: "center",
    });

    // Optional: Add a highlight effect
    element.style.transition = "background-color 0.3s ease";
    const originalBg = element.style.backgroundColor;
    element.style.backgroundColor =
      "var(--nypl-colors-section-research-primary-05)";

    setTimeout(() => {
      element.style.backgroundColor = originalBg;
    }, 2000);
  }
};
