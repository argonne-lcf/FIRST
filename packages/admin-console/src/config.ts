// Registered Globus Auth "thick client" (public client, PKCE)
export const AUTH_CLIENT_ID = "4842f1fc-5abe-4898-b00d-9fb5e226780f";

// FIRST API acts as its own resource server
export const GATEWAY_CLIENT_ID = "681c10cc-f684-4540-bcd7-0b4df3bc26ef";
export const GATEWAY_SCOPE = `https://auth.globus.org/scopes/${GATEWAY_CLIENT_ID}/action_all`;

//  Globus Auth redirect
export const REDIRECT_URI = `${window.location.origin}${import.meta.env.BASE_URL}/callback`;
