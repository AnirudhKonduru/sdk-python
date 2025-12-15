#!/usr/bin/env python3
"""Try to reproduce race using synchronous API calls in threads."""
import multiprocessing
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

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
    import asyncio

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
                    await asyncio.sleep(0.1)  # Small delay
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


def make_sync_call(client, call_id, x, y):
    """Make a synchronous tool call."""
    print(f"[THREAD-{call_id}] Starting call...", flush=True)
    try:
        result = client.call_tool_sync(tool_use_id=call_id, name="calculator", arguments={"x": x, "y": y})
        print(f"[THREAD-{call_id}] Completed: {result['status']}", flush=True)
        return result
    except Exception as e:
        print(f"[THREAD-{call_id}] Exception: {type(e).__name__}: {e}", flush=True)
        return {"error": str(e)}


def main():
    print("[MAIN] Starting services...", flush=True)
    server_thread = threading.Thread(
        target=start_comprehensive_mcp_server, kwargs={"transport": "streamable-http", "port": 9301}, daemon=True
    )
    server_thread.start()

    proxy_process = multiprocessing.Process(
        target=start_5xx_proxy_first_only,
        kwargs={"target_url": "http://127.0.0.1:9301", "proxy_port": 9302},
    )
    proxy_process.start()

    try:
        time.sleep(2)
        print("[MAIN] Services started\n", flush=True)

        def transport_callback() -> MCPTransport:
            return streamablehttp_client(url="http://127.0.0.1:9302/mcp")

        client = MCPClient(transport_callback)
        client.start()

        print("[MAIN] Using SYNCHRONOUS API with thread pool", flush=True)
        print("[MAIN] Launching multiple parallel SYNC calls...\n", flush=True)

        # Use thread pool to make parallel synchronous calls
        # First call will fail and kill background thread
        # Other calls might hit the race window
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(make_sync_call, client, f"call-{i}", i, i + 1)
                for i in range(10)
            ]

            print("[MAIN] Waiting for all calls with 15s timeout...", flush=True)
            time.sleep(2)  # Give first call time to kill the thread

            # Check state
            print(f"\n[MAIN] Thread alive: {client._background_thread.is_alive()}", flush=True)
            print(f"[MAIN] Session set: {client._background_thread_session is not None}", flush=True)
            print(f"[MAIN] Event loop set: {client._background_thread_event_loop is not None}\n", flush=True)

            # Wait for results with timeout
            completed = 0
            hung = 0
            for i, future in enumerate(futures):
                try:
                    result = future.result(timeout=10)
                    completed += 1
                except FuturesTimeoutError:
                    print(f"[MAIN] ✓ Call {i} HUNG (timeout after 10s)", flush=True)
                    hung += 1
                except Exception as e:
                    print(f"[MAIN] Call {i} exception: {e}", flush=True)
                    completed += 1

            print(f"\n[MAIN] Results: {completed} completed, {hung} hung", flush=True)

            if hung > 0:
                print(f"\n[MAIN] ✓✓✓ HANG DETECTED ✓✓✓", flush=True)
                print(f"[MAIN] {hung} call(s) hung indefinitely", flush=True)
                sys.exit(0)  # Success - reproduced the bug
            else:
                print(f"\n[MAIN] ✗ No hangs detected", flush=True)
                print("[MAIN] close_future mechanism prevented hangs", flush=True)
                sys.exit(1)

    finally:
        proxy_process.terminate()
        proxy_process.join()


if __name__ == "__main__":
    main()
