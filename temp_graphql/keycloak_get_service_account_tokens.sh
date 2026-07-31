#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check required environment variables
if [ -z "$KEYCLOAK_REALM" ] || 
   [ -z "$KEYCLOAK_BASE_URL" ] || 
   [ -z "$KEYCLOAK_IMPERSONATION_CLIENT_ID" ] || 
   [ -z "$KEYCLOAK_IMPERSONATION_CLIENT_SECRET" ] ||
   [ -z "$KEYCLOAK_AUDIENCE" ]; then
    echo "Error: Required environment variables not set"
    echo "Please ensure .env file contains:"
    echo "  KEYCLOAK_REALM"
    echo "  KEYCLOAK_BASE_URL"
    echo "  KEYCLOAK_IMPERSONATION_CLIENT_ID"
    echo "  KEYCLOAK_IMPERSONATION_CLIENT_SECRET"
    echo "  KEYCLOAK_AUDIENCE"
    exit 1
fi

response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token" \
 -d 'grant_type=client_credentials' \
 -d "client_id=${KEYCLOAK_IMPERSONATION_CLIENT_ID}" \
 -d "client_secret=${KEYCLOAK_IMPERSONATION_CLIENT_SECRET}")

access_token=$(echo "$response" | jq -r '.access_token')

introspect_response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token/introspect" \
 -H 'Content-Type: application/x-www-form-urlencoded' \
 -d "client_id=${KEYCLOAK_IMPERSONATION_CLIENT_ID}" \
 -d "client_secret=${KEYCLOAK_IMPERSONATION_CLIENT_SECRET}" \
 -d "token=${access_token}")

echo "Impersonation token introspection"
echo "================================="
echo "$introspect_response" | jq .

response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token" \
 -d "client_id=${KEYCLOAK_IMPERSONATION_CLIENT_ID}" \
 -d "client_secret=${KEYCLOAK_IMPERSONATION_CLIENT_SECRET}" \
 -d "subject_token=${access_token}" \
 -d 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
 -d 'requested_token_type=urn:ietf:params:oauth:token-type:access_token' \
 -d "requested_subject=openinference_svc" \
 -d "audience=${KEYCLOAK_AUDIENCE}")

access_token=$(echo "$response" | jq -r '.access_token')

introspect_response=$(curl -s -k -X POST "${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token/introspect" \
 -H 'Content-Type: application/x-www-form-urlencoded' \
 -d "client_id=${KEYCLOAK_IMPERSONATION_CLIENT_ID}" \
 -d "client_secret=${KEYCLOAK_IMPERSONATION_CLIENT_SECRET}" \
 -d "token=${access_token}")

echo ""
echo "User token introspection"
echo "========================="
echo "$introspect_response" | jq .

echo ""
echo "Token response"
echo "=============="
echo "$response" | jq
