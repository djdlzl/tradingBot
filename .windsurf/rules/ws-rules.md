---
trigger: manual
---

- When adding new code and using logging code, use self.logger(utils.trading_logger).
- The trading_upper.py file references the kis_websocket.py file, so do not reference trading_upper.py from kis_websocket.py. Use the function in trading_upper.py using a callback function.
- When writing code, pay attention to indentation. If the indentation is incorrect in Python code, the entire code will result in an error.