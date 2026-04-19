#!/usr/bin/env python3
"""
serve.py — local dev server with HTTP Range request support.

Python's built-in http.server does NOT handle Range requests, which means:
  • The progress bar will show an indeterminate animation (no Content-Length)
  • sql.js-httpvfs would not work

This tiny server adds Range support so local dev matches GitHub Pages behaviour.

Usage:
    python serve.py            # serves web/ on http://localhost:8001
    python serve.py 8080       # custom port
    python serve.py 8001 .     # custom port + directory
"""

import os
import sys
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
DIRECTORY = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "web"


class RangeHTTPRequestHandler(SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler extended with HTTP/1.1 Range support."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DIRECTORY), **kwargs)

    def send_head(self):
        """
        Intercept Range requests before they reach SimpleHTTPRequestHandler.
        Non-Range requests fall through to the parent implementation unchanged.
        """
        range_header = self.headers.get("Range")
        if not range_header:
            return super().send_head()

        # Parse "bytes=start-end"
        try:
            unit, rng = range_header.split("=", 1)
            if unit.strip() != "bytes":
                return super().send_head()
            start_str, end_str = rng.strip().split("-", 1)
        except ValueError:
            self.send_error(416, "Range Not Satisfiable")
            return None

        path = self.translate_path(self.path)
        try:
            f = open(path, "rb")
        except OSError:
            self.send_error(404, "File not found")
            return None

        fs = os.fstat(f.fileno())
        file_size = fs.st_size

        start = int(start_str) if start_str else 0
        end   = int(end_str)   if end_str   else file_size - 1
        end   = min(end, file_size - 1)

        if start > end or start >= file_size:
            f.close()
            self.send_error(416, "Range Not Satisfiable")
            return None

        length = end - start + 1
        f.seek(start)

        self.send_response(206, "Partial Content")
        self.send_header("Content-Type",  self.guess_type(path))
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Range",  f"bytes {start}-{end}/{file_size}")
        self.send_header("Accept-Ranges",  "bytes")
        self.send_header("Last-Modified",  self.date_time_string(int(fs.st_mtime)))
        # CORS headers so the Worker can fetch from localhost
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        return f

    def end_headers(self):
        # Always advertise range support and CORS
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, fmt, *args):
        # Colour-code status codes for readability
        status = str(args[1]) if len(args) > 1 else "?"
        colour = "\033[32m" if status.startswith("2") else \
                 "\033[33m" if status.startswith("3") else "\033[31m"
        reset  = "\033[0m"
        try:
            msg = fmt % args
        except Exception:
            msg = repr((fmt, args))
        print(f"  {colour}{status}{reset}  {msg}")


if __name__ == "__main__":
    server = HTTPServer(("", PORT), RangeHTTPRequestHandler)
    url = f"http://localhost:{PORT}"
    print(f"\n  Vantagepoint Release Notes — local dev server")
    print(f"  Serving: {DIRECTORY.resolve()}")
    print(f"  URL:     {url}")
    print(f"  Range requests: supported  (Python http.server does not support these natively)")
    print(f"\n  Press Ctrl+C to stop.\n")

    # Open browser automatically
    def open_browser():
        import time, webbrowser
        time.sleep(0.3)
        webbrowser.open(url)

    threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")
