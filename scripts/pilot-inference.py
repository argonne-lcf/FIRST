import json
import os

os.environ["ALL_PROXY"] = "socks5h://localhost:1080"
print(f"Setting ALL_PROXY={os.environ['ALL_PROXY']}")

from rich import print

from first_gateway.services.pilot_control import PilotControlClient
from first_gateway.settings import Settings


async def main():
    s = Settings()
    async with s.build_clients() as cs:
        c = PilotControlClient(cs, cn="test")

    url = "https://10.124.186.87:8000/replicas/tara/openai/gpt-oss-20b/replica/3fe49df4/v1/chat/completions"
    async with c._client.stream(
        "POST",
        url,
        json={
            "messages": [
                {"role": "user", "content": "explain the flash attention kernel"}
            ],
            "model": "openai/gpt-oss-20b",
            "stream": True,
        },
    ) as resp:
        async for line in resp.aiter_lines():
            if not line or not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                break
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                continue
            delta = chunk["choices"][0].get("delta", {})
            if text := delta.get("content"):
                print(text, end="", flush=True)


import asyncio

asyncio.run(main())
