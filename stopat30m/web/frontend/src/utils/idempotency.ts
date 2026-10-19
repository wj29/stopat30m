// Keep the same request key on network retries, including after a page reload.
// Scope keys to the signed-in user. No authentication credentials are persisted here.
const pending = new Map<string, { fingerprint: string; key: string }>();
function newKey(): string {
  return typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('');
}
export function requestKey(scope: string, payload: unknown): string {
  const fingerprint = JSON.stringify(payload);
  const cached = pending.get(scope);
  if (cached?.fingerprint === fingerprint) return cached.key;
  try {
    const old = JSON.parse(sessionStorage.getItem(scope) || 'null');
    if (old?.fingerprint === fingerprint && typeof old.key === 'string') { pending.set(scope, old); return old.key; }
    const key = newKey();
    pending.set(scope, { fingerprint, key });
    sessionStorage.setItem(scope, JSON.stringify({ fingerprint, key }));
    return key;
  } catch {
    // Storage may be disabled; the caller still guards synchronous duplicate clicks.
    const key = newKey();
    pending.set(scope, { fingerprint, key });
    return key;
  }
}

export function completeRequest(scope: string) {
  pending.delete(scope);
  try { sessionStorage.removeItem(scope); } catch { /* Storage is optional. */ }
}
