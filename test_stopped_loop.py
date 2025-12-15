#!/usr/bin/env python3
"""Test what happens when run_coroutine_threadsafe is called on a stopped event loop."""
import asyncio
import threading
import time


async def simple_coro():
    print("Coroutine running")
    await asyncio.sleep(0.1)
    return "done"


def background_thread():
    """Start a loop, then stop it."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    print("[BG] Loop created and set")

    # Run the loop briefly then stop it
    async def brief_task():
        await asyncio.sleep(0.1)

    loop.run_until_complete(brief_task())
    print("[BG] Loop completed and stopped")
    # Loop is now stopped but not closed

    # Keep thread alive for a bit
    time.sleep(5)
    print("[BG] Thread exiting")


# Start background thread
thread = threading.Thread(target=background_thread, daemon=True)
thread.start()

# Wait for loop to be created
time.sleep(0.2)

# Get reference to the event loop from background thread
bg_loop = asyncio.all_running_loops()
print(f"[MAIN] Running loops: {bg_loop}")

# Try to submit work to the stopped loop
print("\n[MAIN] Attempting to submit coroutine to stopped loop...")
try:
    # This should hang if the loop is stopped
    future = asyncio.run_coroutine_threadsafe(simple_coro(), thread._target.__code__.co_consts[1])
    print(f"[MAIN] Got future: {future}")
    print("[MAIN] Waiting for result with 2s timeout...")
    result = future.result(timeout=2.0)
    print(f"[MAIN] Result: {result}")
except Exception as e:
    print(f"[MAIN] Exception: {type(e).__name__}: {e}")

thread.join()
