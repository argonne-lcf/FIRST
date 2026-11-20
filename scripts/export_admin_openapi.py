"""
Write the admin API's OpenAPI schema to a file, without running the stack.

The schema comes from the control server's FastAPI app object, which is exactly
what controller-manager serves at http://localhost:9100/openapi.json. Importing
the app needs no database, Redis, env vars or .env file.

Used by the admin console's `npm run gen-sdk` (also `make admin-sdk`):

    uv run python scripts/export_admin_openapi.py packages/admin-console/openapi.json
"""

import json
import sys
from pathlib import Path

from first_gateway.controllers.control_server import app

DEFAULT_OUT = Path(__file__).parents[1] / "packages/admin-console/openapi.json"


def main(out: Path) -> None:
    out.write_text(json.dumps(app.openapi(), indent=2) + "\n")
    print(f"Wrote admin API schema to {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT)
