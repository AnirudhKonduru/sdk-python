#!/usr/bin/env python3
"""Aggressive test to reproduce the race condition by widening the race window."""
import asyncio
import multiprocessing
import sys
import threading
import time

from mcp.client.streamable_http import streamablehttp_client

from strands.tools.mcp.mcp_client import MCPClient
from strands.tools.mcp.mcp_types import MCPTransport


def start_comprehensive_mcp_server(transport: str, port: int):
    """Start a simple MCP server for testing."""
    from mcp.server import FastMCP

    mcp = FastMCP("Test MCP Server", port=port)

    @mcp.tool(description="Calculator tool")
    def calculator(x: int, y: int) -> int:
        time.sleep(0.1)  # Small delay to make timing more predictable
        return x + y

    mcp.run(transport=transport)


def start_5xx_proxy_first_only(target_url: str, proxy_port: int):
    """Proxy that returns 5xx only on first tool call."""
    import aiohttp
    from aiohttp import web

    call_count = {"count": 0}

    async def proxy_handler(request):
        url = f"{target_url}{request.path_qs}"

        async with aiohttp.ClientSession() as session:
            data = await request.read()

            if "tools/call" in f"{data}":
                call_count["count"] += 1
                if call_count["count"] == 1:
                    print(f"[PROXY] Returning 500 for first call", flush=True)
                    return web.Response(status=500, text="Internal Server Error")

            async with session.request(
                method=request.method, url=url, headers=request.headers, data=data, allow_redirects=False
            ) as resp:
                response = web.StreamResponse(status=resp.status, headers=resp.headers)
                await response.prepare(request)

                async for chunk in resp.content.iter_chunked(8192):
                    await response.write(chunk)

                return response

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", proxy_handler)

    web.run_app(app, host="127.0.0.1", port=proxy_port)


async def main():
    print("[MAIN] Starting services...", flush=True)
    server_thread = threading.Thread(
        target=start_comprehensive_mcp_server, kwargs={"transport": "streamable-http", "port": 9201}, daemon=True
    )
    server_thread.start()

    proxy_process = multiprocessing.Process(
        target=start_5xx_proxy_first_only,
        kwargs={"target_url": "http://127.0.0.1:9201", "proxy_port": 9202},
    )
    proxy_process.start()

    try:
        await asyncio.sleep(2)
        print("[MAIN] Services started\n", flush=True)

        def transport_callback() -> MCPTransport:
            return streamablehttp_client(url="http://127.0.0.1:9202/mcp")

        client = MCPClient(transport_callback)
        client.start()

        print("[MAIN] Strategy: Fire first call, wait for thread death, then fire second call", flush=True)
        print("[MAIN] This should hit the race window more reliably\n", flush=True)

        # First call - will fail and kill background thread
        print("[MAIN] Launching first call (will get 5xx and kill thread)...", flush=True)
        first_task = asyncio.create_task(
            client.call_tool_async(tool_use_id="first", name="calculator", arguments={"x": 1, "y": 2})
        )

        # Wait for the first call to complete and thread to die
        await asyncio.sleep(1.5)

        print(f"[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
        print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
        print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}", flush=True)

        if client._background_thread_event_loop:
            print(f"[MAIN] Event loop running: {client._background_thread_event_loop.is_running()}", flush=True)
            print(f"[MAIN] Event loop closed: {client._background_thread_event_loop.is_closed()}", flush=True)

        # Check if we're in the race window
        if (not client._background_thread.is_alive() and
            client._background_thread_session is not None and
            client._background_thread_event_loop is not None):

            print("\n[MAIN] ✓✓✓ PERFECT - WE ARE IN THE RACE WINDOW ✓✓✓", flush=True)
            print("[MAIN] Thread is DEAD but session/loop are still SET", flush=True)
            print("[MAIN] Now launching second call...\n", flush=True)

            # This should trigger the hang
            print("[MAIN] Launching second call (should hang or fail gracefully)...", flush=True)
            second_task = asyncio.create_task(
                client.call_tool_async(tool_use_id="second", name="calculator", arguments={"x": 3, "y": 4})
            )

            try:
                result = await asyncio.wait_for(second_task, timeout=10.0)
                print(f"\n[MAIN] ✗ Second call completed: {result}", flush=True)
                print("[MAIN] Either close_future caught it or the fix is in place", flush=True)
                sys.exit(1)
            except asyncio.TimeoutError:
                print("\n[MAIN] ✓✓✓ HANG CONFIRMED ✓✓✓", flush=True)
                print("[MAIN] Second call hung for 10+ seconds", flush=True)
                print("[MAIN] This proves the race condition exists", flush=True)
                sys.exit(0)
        else:
            print("\n[MAIN] ✗ Race window not present", flush=True)
            print("[MAIN] Thread might still be alive or timing didn't work out", flush=True)
            sys.exit(1)

    finally:
        proxy_process.terminate()
        proxy_process.join()


if __name__ == "__main__":
    asyncio.run(main())
