"""
serve.py — Local dashboard server for 100X Algo
================================================
Run this to open the dashboard in your browser locally.

Usage:
  python serve.py

Then open: http://localhost:8080
"""

import http.server
import socketserver
import webbrowser
import os
import threading

PORT = 8080

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress request logs (cleaner terminal)
        pass

def open_browser():
    import time
    time.sleep(0.8)
    webbrowser.open(f"http://localhost:{PORT}/100x_algo_dashboard.html")

if __name__ == "__main__":
    # Change to the folder where this script lives
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("=" * 50)
    print("  100X Algo — Local Dashboard Server")
    print("=" * 50)
    print(f"  URL: http://localhost:{PORT}/100x_algo_dashboard.html")
    print(f"  Folder: {os.getcwd()}")
    print("  Press Ctrl+C to stop")
    print("=" * 50)

    threading.Thread(target=open_browser, daemon=True).start()

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
