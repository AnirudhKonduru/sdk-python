# MCP Client Race Condition Analysis

## Summary

There is a race condition in `src/strands/tools/mcp/mcp_client.py` that can cause tool calls to hang indefinitely when HTTP errors occur during parallel operations.

## The Bug

**Location:** `_invoke_on_background_thread` method (lines 721-749)

**Root Cause:** Time-of-Check to Time-of-Use (TOCTOU) race condition

### The Race Window

1. **Thread 1:** Calls `call_tool_async`/`call_tool_sync`
2. **Thread 1:** Passes `_is_session_active()` check (line 465/502) - thread is alive ✓
3. **Thread 2:** Gets HTTP 5xx error, triggers `_handle_error_message`
4. **Thread 2:** Exception caught in `_async_background_thread` (line 603)
5. **Thread 2:** Sets `_close_exception` and completes `_close_future`
6. **Thread 2:** Background thread **EXITS**
7. **Thread 1:** Reaches `_invoke_on_background_thread()` (line 474/511)
8. **Thread 1:** Validation at lines 725-730 **PASSES** because:
   - `_background_thread_session` is still set (not None)
   - `_background_thread_event_loop` is still set (not None)
   - `close_future` is still set (not None)
   - **BUT** `_background_thread.is_alive()` would return `False`!
9. **Thread 1:** Calls `asyncio.run_coroutine_threadsafe` (line 748) on a **DEAD** event loop
10. **Thread 1:** The returned future **NEVER COMPLETES** → **INDEFINITE HANG**

### Why This Happens

The state variables (`_background_thread_session`, `_background_thread_event_loop`) are only reset in `stop()` (lines 328-335), which hasn't been called yet. The background thread can die from an exception, but these variables remain set, causing the validation to pass incorrectly.

## Errors That Trigger This

Any exception that propagates to `_async_background_thread` can trigger this:

1. **HTTP 5xx errors** (500, 502, 503, 504) - Most common
2. **HTTP timeout errors**
3. **Network/connection errors** (connection reset, refused, unreachable)
4. **MCP protocol errors** from the ClientSession

## The Fix

Add a thread liveness check to `_invoke_on_background_thread`:

```python
def _invoke_on_background_thread(self, coro: Coroutine[Any, Any, T]) -> futures.Future[T]:
    # save a reference to this so that even if it's reset we have the original
    close_future = self._close_future

    if (
        self._background_thread_session is None
        or self._background_thread_event_loop is None
        or close_future is None
        or self._background_thread is None
        or not self._background_thread.is_alive()  # <-- ADD THIS CHECK
    ):
        raise MCPClientInitializationError("the client session was not initialized")
    # ... rest of method
```

This uses the same logic as `_is_session_active()` to ensure the thread is actually running before submitting work to its event loop.

## Test Created

- **Integration test:** `tests_integ/mcp/test_mcp_client.py::test_race_condition_after_http_error_kills_background_thread`
- **Standalone demo:** `test_race_condition_standalone.py`

The test creates a scenario where:
1. Proxy returns 5xx only on first tool call
2. Multiple parallel calls are made
3. First call kills the background thread
4. Second call exposes the TOCTOU race and hangs

