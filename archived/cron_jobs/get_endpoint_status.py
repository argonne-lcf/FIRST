import os
import re
import time

import globus_compute_sdk
import globus_sdk
from dotenv import load_dotenv

load_dotenv(override=True)

# Load compute endpoint details
# This creates a dictionary in the form of {"endpoint_name": "uuid"}
ENDPOINTS = re.split(r"[\s;]+", os.getenv("ENDPOINTS").strip())
ENDPOINTS = {item.split(":")[0]: item.split(":")[1] for item in ENDPOINTS}

# Load compute endpoint credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# If this file is executed as a script ...
if __name__ == "__main__":
    # Create a Globus Compute client
    gcc = globus_compute_sdk.Client(
        app=globus_sdk.ClientApp(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    )

    # For each compute endpoint ...
    for endpoint_name, endpoint_id in ENDPOINTS.items():
        # Print the endpoint name along with its status
        print(endpoint_name, gcc.get_endpoint_status(endpoint_id)["status"])

        # Sleep to avoid rate-limit errors from Globus API
        time.sleep(1)
