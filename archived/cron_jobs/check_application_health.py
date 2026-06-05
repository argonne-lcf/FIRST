#!/usr/bin/env python3
"""
Application Health Check Script

This script checks the health of the Inference Gateway application by:
1. Checking the application /health endpoint
2. Checking Redis connectivity
3. Checking PostgreSQL connectivity
4. Checking Globus Compute connectivity
5. Sending alerts if any component is unhealthy

Designed to run as a cron job every 5 minutes.
"""

import json
import logging
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.text import MIMEText

import requests

# Add parent directory to path to import Django modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inference_gateway.settings")
import django

django.setup()

import resource_server_async.globus_utils as globus_utils
from django.core.cache import cache
from django.db import connection
from resource_server_async.models import Endpoint

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/tmp/application_health.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


class ApplicationHealthChecker:
    """Check health of application components"""

    def __init__(self):
        # Email configuration from environment
        self.alert_email_to = os.getenv("ALERT_EMAIL_TO", "").split()
        # Default to first recipient as sender if not specified (ANL blocks noreply addresses)
        default_from = (
            self.alert_email_to[0]
            if self.alert_email_to
            else "noreply@inference-gateway"
        )
        self.alert_email_from = os.getenv("ALERT_EMAIL_FROM", default_from)

        log.info(
            f"Email configuration loaded: FROM={self.alert_email_from}, TO={self.alert_email_to}"
        )

        # SMTP configuration (optional - if not set, will use sendmail)
        self.smtp_host = os.getenv("SMTP_HOST", "")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.smtp_use_tls = os.getenv("SMTP_USE_TLS", "True").lower() == "true"

        # Application URL for health check (default to localhost)
        self.application_url = os.getenv(
            "STREAMING_SERVER_HOST", "http://localhost:8000"
        )

    def check_redis(self) -> dict:
        """Check Redis connectivity"""
        try:
            # Try to set and get a test value
            test_key = "health_check_test"
            test_value = f"test_{datetime.now().timestamp()}"

            cache.set(test_key, test_value, 60)
            retrieved_value = cache.get(test_key)

            if retrieved_value == test_value:
                cache.delete(test_key)
                return {
                    "component": "Redis",
                    "status": "healthy",
                    "message": "Redis connection successful",
                }
            else:
                return {
                    "component": "Redis",
                    "status": "unhealthy",
                    "error": "Redis get/set test failed - values do not match",
                }
        except Exception as e:
            return {
                "component": "Redis",
                "status": "unhealthy",
                "error": f"Redis connection failed: {str(e)}",
            }

    def check_postgres(self) -> dict:
        """Check PostgreSQL connectivity"""
        try:
            # Try a simple database query
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()

                if result and result[0] == 1:
                    # Also check if we can query a table
                    endpoint_count = Endpoint.objects.count()
                    return {
                        "component": "PostgreSQL",
                        "status": "healthy",
                        "message": f"PostgreSQL connection successful (found {endpoint_count} endpoints)",
                    }
                else:
                    return {
                        "component": "PostgreSQL",
                        "status": "unhealthy",
                        "error": "PostgreSQL query returned unexpected result",
                    }
        except Exception as e:
            return {
                "component": "PostgreSQL",
                "status": "unhealthy",
                "error": f"PostgreSQL connection failed: {str(e)}",
            }

    def check_globus_compute(self) -> dict:
        """Check Globus Compute connectivity"""
        try:
            # Try to create a Globus Compute client
            gcc = globus_utils.get_compute_client_from_globus_app()

            # Try to get executor
            gce = globus_utils.get_compute_executor(client=gcc)

            if gcc and gce:
                return {
                    "component": "Globus Compute",
                    "status": "healthy",
                    "message": "Globus Compute client and executor initialized successfully",
                }
            else:
                return {
                    "component": "Globus Compute",
                    "status": "unhealthy",
                    "error": "Failed to initialize Globus Compute client or executor",
                }
        except Exception as e:
            return {
                "component": "Globus Compute",
                "status": "unhealthy",
                "error": f"Globus Compute initialization failed: {str(e)}",
            }

    def check_application_health_endpoint(self) -> dict:
        """Check if the application /health endpoint is responding"""
        try:
            health_url = f"http://{self.application_url}/resource_server_async/health"
            # Set a reasonable timeout (5 seconds)
            response = requests.get(health_url, timeout=5)

            # Check if we got a 200 status code
            if response.status_code == 200:
                # Try to parse the JSON response
                try:
                    data = response.json()
                    if data.get("status") == "ok":
                        return {
                            "component": "Application /health Endpoint",
                            "status": "healthy",
                            "message": f"Application responding successfully at {health_url}",
                        }
                    else:
                        return {
                            "component": "Application /health Endpoint",
                            "status": "unhealthy",
                            "error": f"Unexpected response from health endpoint: {data}",
                        }
                except Exception as e:
                    return {
                        "component": "Application /health Endpoint",
                        "status": "unhealthy",
                        "error": f"Failed to parse health endpoint JSON response: {str(e)}",
                    }
            else:
                return {
                    "component": "Application /health Endpoint",
                    "status": "unhealthy",
                    "error": f"Health endpoint returned status code {response.status_code}",
                }
        except requests.exceptions.Timeout:
            return {
                "component": "Application /health Endpoint",
                "status": "unhealthy",
                "error": f"Health endpoint request timed out after 5 seconds at {health_url}",
            }
        except requests.exceptions.ConnectionError as e:
            return {
                "component": "Application /health Endpoint",
                "status": "unhealthy",
                "error": f"Failed to connect to health endpoint at {health_url}: {str(e)}",
            }
        except Exception as e:
            return {
                "component": "Application /health Endpoint",
                "status": "unhealthy",
                "error": f"Health endpoint check failed: {str(e)}",
            }

    def check_all_components(self) -> dict:
        """Check health of all application components"""
        results = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "components": [],
        }

        log.info("Checking Application /health endpoint...")
        app_health_result = self.check_application_health_endpoint()
        results["components"].append(app_health_result)
        if app_health_result["status"] != "healthy":
            results["overall_status"] = "unhealthy"

        log.info("Checking Redis...")
        redis_result = self.check_redis()
        results["components"].append(redis_result)
        if redis_result["status"] != "healthy":
            results["overall_status"] = "unhealthy"

        log.info("Checking PostgreSQL...")
        postgres_result = self.check_postgres()
        results["components"].append(postgres_result)
        if postgres_result["status"] != "healthy":
            results["overall_status"] = "unhealthy"

        log.info("Checking Globus Compute...")
        globus_result = self.check_globus_compute()
        results["components"].append(globus_result)
        if globus_result["status"] != "healthy":
            results["overall_status"] = "unhealthy"

        return results

    def send_email_via_smtp(self, subject: str, body: str) -> bool:
        """Send email via SMTP"""
        try:
            log.info(
                f"Sending email via SMTP (host: {self.smtp_host}:{self.smtp_port})"
            )
            log.info(f"From: {self.alert_email_from}")
            log.info(f"To: {self.alert_email_to}")

            # Create message - use simple MIMEText instead of MIMEMultipart
            msg = MIMEText(body, "plain", "utf-8")
            msg["From"] = self.alert_email_from
            msg["To"] = ", ".join(self.alert_email_to)
            msg["Subject"] = subject

            # Connect and send
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.smtp_use_tls:
                log.info("Starting TLS...")
                server.starttls()

            if self.smtp_user and self.smtp_password:
                log.info(f"Authenticating as {self.smtp_user}...")
                server.login(self.smtp_user, self.smtp_password)

            log.info("Sending email...")
            server.sendmail(self.alert_email_from, self.alert_email_to, msg.as_string())
            server.quit()

            log.info(
                f"✓ Email sent successfully via SMTP to {', '.join(self.alert_email_to)}"
            )
            return True

        except Exception as e:
            log.error(f"❌ Failed to send email via SMTP: {e}", exc_info=True)
            return False

    def send_email_via_sendmail(self, email_content: str) -> bool:
        """Send email via sendmail command"""
        try:
            # Write email content to temporary file
            email_file = "/tmp/application_health_alert_email.txt"
            log.info(f"Writing email content to {email_file}")
            with open(email_file, "w") as f:
                f.write(email_content)
            log.info("✓ Email content written successfully")

            # Send email using sendmail
            recipients = " ".join(self.alert_email_to)
            sendmail_cmd = f"sendmail {recipients} < {email_file}"
            log.info(f"Executing sendmail command: {sendmail_cmd}")

            result = subprocess.run(
                sendmail_cmd, shell=True, capture_output=True, text=True
            )

            log.info(f"Sendmail return code: {result.returncode}")
            if result.stdout:
                log.info(f"Sendmail stdout: {result.stdout}")
            if result.stderr:
                log.info(f"Sendmail stderr: {result.stderr}")

            if result.returncode == 0:
                log.info(
                    f"✓ Alert email queued successfully via sendmail to {recipients}"
                )
                log.info("⚠️  NOTE: Check mail queue with 'mailq' to verify delivery")
                return True
            else:
                log.error(
                    f"❌ Failed to send via sendmail (return code {result.returncode})"
                )
                return False

        except Exception as e:
            log.error(f"❌ Failed to send email via sendmail: {e}", exc_info=True)
            return False

    def send_alert_email(self, results: dict):
        """Send email alert if application is unhealthy"""
        log.info("=" * 80)
        log.info("ENTERING send_alert_email()")
        log.info(f"Overall status: {results['overall_status']}")
        log.info(f"Alert email recipients configured: {self.alert_email_to}")
        log.info(f"SMTP configured: {bool(self.smtp_host)}")
        log.info("=" * 80)

        if results["overall_status"] == "healthy":
            log.info("All application components are healthy. No alert email needed.")
            return

        log.warning("⚠️  APPLICATION UNHEALTHY - Attempting to send alert email")

        if not self.alert_email_to or not any(self.alert_email_to):
            log.error(
                "❌ CANNOT SEND EMAIL: No alert email recipients configured (ALERT_EMAIL_TO is empty)"
            )
            log.error(
                f"ALERT_EMAIL_TO value: '{os.getenv('ALERT_EMAIL_TO', 'NOT SET')}'"
            )
            return

        try:
            # Build email content (plain text only - no special chars to avoid spam filters)
            unhealthy_components = [
                c for c in results["components"] if c["status"] != "healthy"
            ]
            log.info(f"Found {len(unhealthy_components)} unhealthy component(s)")

            subject = f"Application Health Alert - {len(unhealthy_components)} Component(s) Unhealthy"

            body = f"""Application Health Monitoring Alert
====================================

Timestamp: {results["timestamp"]}
Overall Status: {results["overall_status"].upper()}

COMPONENT STATUS:
"""

            for component in results["components"]:
                # Use plain ASCII characters to avoid spam filters
                status_icon = "[OK]" if component["status"] == "healthy" else "[FAIL]"
                body += f"\n{status_icon} {component['component']}: {component['status'].upper()}\n"

                if "message" in component:
                    body += f"   Message: {component['message']}\n"
                if "error" in component:
                    body += f"   Error: {component['error']}\n"

            body += """

---
This is an automated alert from the Inference Gateway Application Health Monitor.
"""

            log.info(f"Email content preview:\n{body[:500]}...")

            # Try SMTP first if configured, fall back to sendmail
            if self.smtp_host:
                success = self.send_email_via_smtp(subject, body)
                if success:
                    return
                log.warning("SMTP failed, falling back to sendmail...")

            # Use sendmail (with Subject in content)
            email_content = f"Subject: {subject}\n\n{body}"
            self.send_email_via_sendmail(email_content)

        except Exception as e:
            log.error(f"❌ Exception in send_alert_email: {e}", exc_info=True)

    def run(self):
        """Main health check"""
        log.info("=" * 80)
        log.info("Starting Application Health Check")
        log.info("=" * 80)

        try:
            results = self.check_all_components()

            # Log summary
            log.info("-" * 80)
            log.info(f"Overall Status: {results['overall_status'].upper()}")
            for component in results["components"]:
                status_icon = "✓" if component["status"] == "healthy" else "✗"
                log.info(
                    f"  {status_icon} {component['component']}: {component['status']}"
                )
                if component["status"] != "healthy" and "error" in component:
                    log.error(f"      Error: {component['error']}")
            log.info("-" * 80)

            # Send alert email if needed
            log.info("Calling send_alert_email()...")
            self.send_alert_email(results)
            log.info("send_alert_email() completed")

            # Save results to file
            results_file = "/tmp/application_health_last_check.json"
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)
            log.info(f"Results saved to {results_file}")

            return results

        except Exception as e:
            log.error(f"Error in health check: {e}", exc_info=True)
            raise


def main():
    """Main entry point"""
    checker = ApplicationHealthChecker()
    results = checker.run()

    # Exit with error code if application is unhealthy
    if results["overall_status"] != "healthy":
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
