#!/usr/bin/env python3
"""Test what happens when run_coroutine_threadsafe is called on a stopped event loop."""
import asyncio
import threading
import time
from concurrent import futures


class LoopManager:
    def __init__(self):
        self.loop = None
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        time.sleep(0.5)  # Wait for loop to start

    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        print(f"[THREAD] Loop started, thread={threading.current_thread().name}")

        # Run a brief task then exit (loop stops)
        async def brief_task():
            await asyncio.sleep(0.2)
            print("[THREAD] Brief task complete, loop will now stop")

        self.loop.run_until_complete(brief_task())
        print(f"[THREAD] Loop stopped, thread alive={self.thread.is_alive()}")
        # Loop is now stopped, thread will exit
        print("[THREAD] Thread exiting")

    def submit_after_stop(self):
        """Try to submit work after the loop has stopped."""
        print(f"\n[MAIN] Thread alive: {self.thread.is_alive()}")
        print(f"[MAIN] Loop set: {self.loop is not None}")
        print(f"[MAIN] Loop running: {self.loop.is_running() if self.loop else 'N/A'}")
        print(f"[MAIN] Loop closed: {self.loop.is_closed() if self.loop else 'N/A'}")

        async def test_coro():
            print("[CORO] This should never print if loop is stopped")
            return "done"

        print("\n[MAIN] Attempting to submit coroutine to stopped loop...")
        future = asyncio.run_coroutine_threadsafe(test_coro(), self.loop)
        print(f"[MAIN] Got future: {future}")
        print("[MAIN] Waiting for result with 3s timeout...")

        try:
            result = future.result(timeout=3.0)
            print(f"[MAIN] ✗ Got result: {result}")
            return False
        except futures.TimeoutError:
            print("[MAIN] ✓✓✓ TIMEOUT - Future never completed!")
            print("[MAIN] This proves run_coroutine_threadsafe on stopped loop HANGS")
            return True
        except Exception as e:
            print(f"[MAIN] Got exception: {type(e).__name__}: {e}")
            return False


if __name__ == "__main__":
    manager = LoopManager()
    manager.start()

    # Wait for thread to exit (loop to stop)
    manager.thread.join(timeout=2.0)

    # Now try to submit work
    hung = manager.submit_after_stop()

    if hung:
        print("\n✓ Confirmed: Submitting to stopped loop causes indefinite hang")
        exit(0)
    else:
        print("\n✗ Unexpectedly completed or failed differently")
        exit(1)
