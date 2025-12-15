#!/usr/bin/env python3
"""Test with MANY parallel tool calls to trigger the race condition more reliably."""
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


def start_5xx_proxy_for_some_calls(target_url: str, proxy_port: int, fail_every: int = 3):
    """Proxy that returns 5xx for every Nth tool call."""
    import aiohttp
    from aiohttp import web

    call_count = {"count": 0}

    async def proxy_handler(request):
        url = f"{target_url}{request.path_qs}"

        async with aiohttp.ClientSession() as session:
            data = await request.read()

            if "tools/call" in f"{data}":
                call_count["count"] += 1
                # Fail every Nth call
                if call_count["count"] % fail_every == 0:
                    print(f"[PROXY] Returning 500 for call #{call_count['count']}", flush=True)
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
    print("[MAIN] Starting MCP server...", flush=True)
    server_thread = threading.Thread(
        target=start_comprehensive_mcp_server, kwargs={"transport": "streamable-http", "port": 9101}, daemon=True
    )
    server_thread.start()

    print("[MAIN] Starting proxy...", flush=True)
    proxy_process = multiprocessing.Process(
        target=start_5xx_proxy_for_some_calls,
        kwargs={"target_url": "http://127.0.0.1:9101", "proxy_port": 9102, "fail_every": 5},
    )
    proxy_process.start()

    try:
        await asyncio.sleep(2)
        print("[MAIN] Services started\n", flush=True)

        def transport_callback() -> MCPTransport:
            return streamablehttp_client(url="http://127.0.0.1:9102/mcp")

        client = MCPClient(transport_callback)
        client.start()

        # Launch MANY parallel calls to increase race window probability
        num_calls = 50
        print(f"[MAIN] Launching {num_calls} PARALLEL tool calls...", flush=True)
        print("[MAIN] Every 5th call will get 5xx error and potentially kill background thread\n", flush=True)

        tasks = [
            asyncio.create_task(client.call_tool_async(tool_use_id=f"call_{i}", name="calculator", arguments={"x": i, "y": i + 1}))
            for i in range(num_calls)
        ]

        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=30.0)

            print(f"\n[MAIN] All {num_calls} calls completed", flush=True)

            # Count successes and errors
            errors = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "error")
            exceptions = sum(1 for r in results if isinstance(r, Exception))
            successes = num_calls - errors - exceptions

            print(f"[MAIN] Successes: {successes}, Errors: {errors}, Exceptions: {exceptions}", flush=True)

            # Check if race window was present
            if not client._background_thread.is_alive() and client._background_thread_session is not None:
                print("\n[MAIN] ✓ RACE CONDITION WINDOW WAS PRESENT", flush=True)
                print(f"[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
                print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
                print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}", flush=True)
                print("[MAIN] But all calls completed - close_future mechanism prevented hang", flush=True)
                sys.exit(0)  # This is actually good - the mechanism is working
            else:
                print("\n[MAIN] ✗ Race window not detected", flush=True)
                sys.exit(1)

        except asyncio.TimeoutError:
            print(f"\n[MAIN] ✓✓✓ HANG DETECTED ✓✓✓", flush=True)
            print(f"[MAIN] Some of the {num_calls} parallel calls hung!", flush=True)
            print(f"\n[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
            print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
            print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}", flush=True)
            sys.exit(0)  # Bug confirmed

    finally:
        proxy_process.terminate()
        proxy_process.join()


if __name__ == "__main__":
    asyncio.run(main())
