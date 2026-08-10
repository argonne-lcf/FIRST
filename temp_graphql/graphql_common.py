import json
import os

import requests
from dotenv import load_dotenv

# Environment variables for testing
load_dotenv()
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "")
KEYCLOAK_ACCESS_TOKEN = os.getenv("KEYCLOAK_ACCESS_TOKEN", "")
SSL_VERIFY = os.getenv("SSL_VERIFY", "True").lower() in ("true", "1", "t")

# Setup request arguments
headers = {
    "Authorization": f"Bearer {KEYCLOAK_ACCESS_TOKEN}",
    "Content-Type": "application/json",
}


# Generic command to send post requests to GraphQL
def post_graphql(payload=None):
    """Generic command to send post requests to GraphQL."""

    try:
        # Send API request to GraphQL
        response = requests.post(
            GRAPHQL_URL, json=payload, headers=headers, verify=SSL_VERIFY
        )
        response = response.json()
        print(json.dumps(response, indent=2))

    # Error message if it did not work
    except Exception as e:
        raise Exception(f"Error: {e}")
