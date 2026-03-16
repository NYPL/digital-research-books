export function getOrCreateSessionId(): string {
  let sessionId = sessionStorage.getItem("vra_session_id");
  if (!sessionId) {
    sessionId = crypto.randomUUID();
    sessionStorage.setItem("vra_session_id", sessionId);
  }
  return sessionId;
}
