#!/usr/bin/env python3
"""Monkey-patch to artificially widen the race window and force the hang."""
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
        target=start_comprehensive_mcp_server, kwargs={"transport": "streamable-http", "port": 9401}, daemon=True
    )
    server_thread.start()

    proxy_process = multiprocessing.Process(
        target=start_5xx_proxy_first_only,
        kwargs={"target_url": "http://127.0.0.1:9401", "proxy_port": 9402},
    )
    proxy_process.start()

    try:
        await asyncio.sleep(2)
        print("[MAIN] Services started\n", flush=True)

        def transport_callback() -> MCPTransport:
            return streamablehttp_client(url="http://127.0.0.1:9402/mcp")

        client = MCPClient(transport_callback)

        # Monkey-patch to add delay between _is_session_active check and actual invocation
        # This artificially widens the race window
        original_invoke = client._invoke_on_background_thread

        def patched_invoke(coro):
            print("[PATCH] _invoke_on_background_thread called", flush=True)
            # Add a 2-second delay here
            # During this time, if the thread dies, we'll hit the race condition
            time.sleep(2)
            print(f"[PATCH] After delay - thread alive: {client._background_thread.is_alive()}", flush=True)
            print(f"[PATCH] After delay - loop running: {client._background_thread_event_loop.is_running() if client._background_thread_event_loop else 'N/A'}", flush=True)
            return original_invoke(coro)

        client._invoke_on_background_thread = patched_invoke

        client.start()

        print("[MAIN] MONKEY-PATCH ACTIVE: 2s delay in _invoke_on_background_thread", flush=True)
        print("[MAIN] This should force the race condition\n", flush=True)

        # Launch two calls in parallel
        # First will fail and kill thread
        # Second will hit the patched delay, and during that delay the thread will die
        print("[MAIN] Launching parallel calls...", flush=True)
        tasks = [
            asyncio.create_task(
                client.call_tool_async(tool_use_id="first", name="calculator", arguments={"x": 1, "y": 2})
            ),
            asyncio.create_task(
                client.call_tool_async(tool_use_id="second", name="calculator", arguments={"x": 3, "y": 4})
            ),
        ]

        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=15.0)
            print(f"\n[MAIN] ✗ Both calls completed: {len([r for r in results if isinstance(r, dict)])} dicts, {len([r for r in results if isinstance(r, Exception)])} exceptions", flush=True)
            sys.exit(1)
        except asyncio.TimeoutError:
            print("\n[MAIN] ✓✓✓ HANG DETECTED WITH MONKEY-PATCH ✓✓✓", flush=True)
            print("[MAIN] The 2s delay forced the thread to die during the race window", flush=True)
            print(f"[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
            sys.exit(0)

    finally:
        proxy_process.terminate()
        proxy_process.join()


if __name__ == "__main__":
    asyncio.run(main())
