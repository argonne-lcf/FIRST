#!/usr/bin/env python3

import os
import sys
import json
import getpass
import requests
from pathlib import Path
from dotenv import load_dotenv
import urllib3

# Disable SSL warnings when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables from .env file
load_dotenv()

# Get required environment variables
KEYCLOAK_REALM = os.getenv('KEYCLOAK_REALM')
KEYCLOAK_BASE_URL = os.getenv('KEYCLOAK_BASE_URL')
KEYCLOAK_PUBLIC_CLIENT_ID = os.getenv('KEYCLOAK_PUBLIC_CLIENT_ID')

# Check required environment variables
if not all([KEYCLOAK_REALM, KEYCLOAK_BASE_URL, KEYCLOAK_PUBLIC_CLIENT_ID]):
    print("Error: Required environment variables not set")
    print("Please ensure .env file contains:")
    print("  KEYCLOAK_REALM")
    print("  KEYCLOAK_BASE_URL")
    print("  KEYCLOAK_PUBLIC_CLIENT_ID")
    sys.exit(1)

# Get credentials from user
username = input("username: ")
password = getpass.getpass("cryptoauth: ")

# Prepare token request
url = f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"
data = {
    'grant_type': 'password',
    'client_id': KEYCLOAK_PUBLIC_CLIENT_ID,
    'username': username,
    'password': password
}

# Make request
try:
    response = requests.post(url, data=data)
    response.raise_for_status()
    
    # Pretty print JSON response
    print(json.dumps(response.json(), indent=2))
    
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")
    if hasattr(e.response, 'text'):
        try:
            error_json = json.loads(e.response.text)
            print(json.dumps(error_json, indent=2))
        except json.JSONDecodeError:
            print(e.response.text)
    sys.exit(1)