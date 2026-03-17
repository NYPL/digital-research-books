export function getOrCreateSessionId(): string {
  let sessionId = sessionStorage.getItem("vraSessionId");
  if (!sessionId) {
    if (typeof crypto.randomUUID === "function") {
      sessionId = crypto.randomUUID();
    } else {
      sessionId = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
        /[xy]/g,
        (c) => {
          const r = (Math.random() * 16) | 0;
          const v = c === "x" ? r : (r & 0x3) | 0x8;
          return v.toString(16);
        }
      );
    }
    sessionStorage.setItem("vraSessionId", sessionId);
  }
  return sessionId;
}
