#!/bin/bash

# Get to the working directory
cd /home/webportal/inference-gateway/cron_jobs

# Email addresses that will receive endpoint-down status alert
admin_emails="bcote@anl.gov atanikanti@anl.gov"

# Collect the status of all endpoints in the .env file
# This will print one line per endpoint (endpoint_name endpoint_status)
data=$(/home/webportal/inference-gateway/.venv/bin/python get_endpoint_status.py)

# Filter lines based on the status
offline_endpoints=$(echo "$data" | grep 'offline')
online_endpoints=$(echo "$data" | grep 'online')

# Draft email to alert admins
{
    echo "Subject: compute endpoints offline"
    echo ""
    echo "Offline endpoints:"
    echo "$offline_endpoints"
    echo ""
    echo "Online endpoints:"
    echo "$online_endpoints"
} > email.out

# display the email content (for testing)
cat email.out

# Send email to admins if endpoints are offline ...
if [[ -n "$offline_endpoints" ]]; then
    sendmail ${admin_emails} < email.out
fi
