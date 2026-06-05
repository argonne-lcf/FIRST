"""Minimal mock OpenAI Chat Completions server."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

RESPONSE = json.dumps(
    {
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": 0,
        "model": "mock-1",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "This is a mock response."},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
).encode()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if cl := self.headers.get("Content-Length"):
            self.rfile.read(int(cl))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(RESPONSE))
        self.end_headers()
        self.wfile.write(RESPONSE)

    do_GET = do_POST


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 8080), Handler).serve_forever()
