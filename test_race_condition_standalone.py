#!/usr/bin/env python3
"""Standalone script to reproduce the MCP client race condition.

This script demonstrates the race condition where:
1. Background thread dies from an HTTP 5xx error
2. A subsequent tool call is made before stop() resets state
3. The call passes validation but hangs because event loop is dead
"""
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
        return x + y

    mcp.run(transport=transport)


def start_5xx_proxy_for_first_tool_call_only(target_url: str, proxy_port: int):
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
                    print(f"[PROXY] Returning 500 for first tool call", flush=True)
                    return web.Response(status=500, text="Internal Server Error")
                else:
                    print(f"[PROXY] Proxying tool call #{call_count['count']}", flush=True)

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
    print("[MAIN] Starting MCP server...", flush=True)
    server_thread = threading.Thread(
        target=start_comprehensive_mcp_server, kwargs={"transport": "streamable-http", "port": 9001}, daemon=True
    )
    server_thread.start()

    print("[MAIN] Starting proxy...", flush=True)
    proxy_process = multiprocessing.Process(
        target=start_5xx_proxy_for_first_tool_call_only,
        kwargs={"target_url": "http://127.0.0.1:9001", "proxy_port": 9002},
    )
    proxy_process.start()

    try:
        await asyncio.sleep(2)  # Wait for services to start
        print("[MAIN] Services started", flush=True)

        def transport_callback() -> MCPTransport:
            return streamablehttp_client(url="http://127.0.0.1:9002/mcp")

        client = MCPClient(transport_callback)

        # Start the client
        client.start()

        print("[MAIN] Making PARALLEL tool calls...", flush=True)
        print("[MAIN] First will fail with 5xx and kill background thread", flush=True)
        print("[MAIN] Second will hit TOCTOU race if timing is right\n", flush=True)

        # Launch both calls in parallel to hit the TOCTOU race condition:
        # Call 1: Passes _is_session_active() check, starts executing
        # Call 1: Gets 5xx error, kills background thread
        # Call 2: Passes _is_session_active() check (thread still alive at that moment)
        # Call 2: Reaches _invoke_on_background_thread but thread is NOW dead -> HANG

        tasks = [
            asyncio.create_task(
                client.call_tool_async(tool_use_id="first", name="calculator", arguments={"x": 1, "y": 2})
            ),
            asyncio.create_task(
                client.call_tool_async(tool_use_id="second", name="calculator", arguments={"x": 3, "y": 4})
            ),
        ]

        try:
            # Wait for both with a timeout
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=30.0)
            print(f"\n[MAIN] Both calls completed: {results}", flush=True)

            # Check if we saw the race condition window
            if not client._background_thread.is_alive() and client._background_thread_session is not None:
                print("\n[MAIN] ✓ RACE CONDITION WINDOW WAS PRESENT", flush=True)
                print(f"[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
                print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
                print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}", flush=True)
                print("[MAIN] But the calls completed - the timing didn't trigger the hang", flush=True)

            print("\n[MAIN] ✗ BUG NOT REPRODUCED in this run", flush=True)
            print("[MAIN] The TOCTOU race exists but the timing didn't align to cause a hang", flush=True)
            sys.exit(1)
        except asyncio.TimeoutError:
            print("\n[MAIN] ✓✓✓ RACE CONDITION CONFIRMED - HANG DETECTED ✓✓✓", flush=True)

            # Check the state
            print(f"\n[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
            print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
            print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}", flush=True)

            print("\n[MAIN] Root cause - TOCTOU (Time-of-Check to Time-of-Use):", flush=True)
            print("[MAIN]   1. Call 2 passed _is_session_active() check (thread was alive)", flush=True)
            print("[MAIN]   2. Call 1's 5xx error killed background thread", flush=True)
            print("[MAIN]   3. Call 2 reached _invoke_on_background_thread with dead thread", flush=True)
            print("[MAIN]   4. Validation passed (session/event_loop still set)", flush=True)
            print("[MAIN]   5. asyncio.run_coroutine_threadsafe submitted to DEAD loop", flush=True)
            print("[MAIN]   6. Result: INDEFINITE HANG", flush=True)
            sys.exit(0)  # Success - we reproduced the bug
        except Exception as e:
            print(f"\n[MAIN] Got exception: {type(e).__name__}: {e}", flush=True)
            sys.exit(1)

    finally:
        proxy_process.terminate()
        proxy_process.join()


if __name__ == "__main__":
    asyncio.run(main())
