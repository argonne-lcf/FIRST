#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check required environment variables
if [ -z "$KEYCLOAK_REALM" ] || 
   [ -z "$KEYCLOAK_BASE_URL" ] || 
   [ -z "$KEYCLOAK_PUBLIC_CLIENT_ID" ] ||
   [ -z "$KEYCLOAK_IMPERSONATION_CLIENT_ID" ] ||
   [ -z "$KEYCLOAK_IMPERSONATION_CLIENT_SECRET" ]; then
    echo "Error: Required environment variables not set"
    echo "Please ensure .env file contains:"
    echo "  KEYCLOAK_REALM"
    echo "  KEYCLOAK_BASE_URL"
    echo "  KEYCLOAK_PUBLIC_CLIENT_ID"
    echo "  KEYCLOAK_IMPERSONATION_CLIENT_ID"
    echo "  KEYCLOAK_IMPERSONATION_CLIENT_SECRET"
    exit 1
fi

echo "User credentials"
echo "================"
read -sp "username: " username
echo
read -sp "cryptoauth: " passvar

response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token" \
 -H 'Content-Type: application/x-www-form-urlencoded' \
 -d 'grant_type=password' \
 -d "client_id=${KEYCLOAK_PUBLIC_CLIENT_ID}" \
 -d "username=${username}" \
 -d "password=${passvar}")

access_token=$(echo "$response" | jq -r '.access_token')

introspect_response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token/introspect" \
 -H 'Content-Type: application/x-www-form-urlencoded' \
 -d "client_id=${KEYCLOAK_IMPERSONATION_CLIENT_ID}" \
 -d "client_secret=${KEYCLOAK_IMPERSONATION_CLIENT_SECRET}" \
 -d "token=${access_token}")

echo "Introspection"
echo "============="
echo "$introspect_response" | jq .

echo ""
echo "Token response"
echo "=============="
echo "$response" | jq