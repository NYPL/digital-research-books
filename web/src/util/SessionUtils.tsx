export function getOrCreateSessionId(): string {
  let sessionId = sessionStorage.getItem("vraSessionId");
  if (!sessionId) {
    sessionId = crypto.randomUUID();
    sessionStorage.setItem("vraSessionId", sessionId);
  }
  return sessionId;
}
