// Tiny REST helper for the Messaging Service public API.
export function createApi(baseUrl) {
  async function call(method, path, { body, token } = {}) {
    const res = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { 'content-type': 'application/json', ...(token && { authorization: `Bearer ${token}` }) },
      body: body ? JSON.stringify(body) : undefined,
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw Object.assign(new Error(`${method} ${path} -> ${res.status} ${json.error ?? ''}`), { status: res.status, body: json });
    return json;
  }

  return {
    createUser: (name) => call('POST', '/users', { body: { name } }),
    registerDevice: (userId, platform, pushToken) =>
      call('POST', `/users/${userId}/devices`, { body: { platform, push_token: pushToken } }),
    conversations: (token) => call('GET', '/conversations', { token }),
    createDirect: (token, userId) => call('POST', '/conversations/direct', { token, body: { user_id: userId } }),
    createGroup: (token, name, members) => call('POST', '/conversations/group', { token, body: { name, members } }),
    addMember: (token, conversationId, userId) =>
      call('POST', `/conversations/${encodeURIComponent(conversationId)}/members`, { token, body: { user_id: userId } }),
    history: (token, conversationId, beforeSeq, limit) =>
      call('GET', `/conversations/${encodeURIComponent(conversationId)}/messages?${new URLSearchParams({
        ...(beforeSeq && { before_seq: beforeSeq }), ...(limit && { limit }) })}`, { token }),
    presence: (token, userId) => call('GET', `/presence/${userId}`, { token }),
    uploadUrl: (token, contentType, size) => call('POST', '/media/upload-url', { token, body: { content_type: contentType, size } }),
    downloadUrl: (token, key) => call('GET', `/media/download-url?key=${encodeURIComponent(key)}`, { token }),
  };
}
