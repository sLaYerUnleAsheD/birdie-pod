import http.server
import socketserver
import os
import sys

PORT = int(os.environ.get("PORT", 8000))
viewer_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viewer")
os.chdir(viewer_dir)

class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

if __name__ == "__main__":
    socketserver.TCPServer.allow_reuse_address = True
    try:
        httpd = socketserver.TCPServer(("0.0.0.0", PORT), NoCacheHandler)
    except Exception:
        httpd = socketserver.TCPServer(("127.0.0.1", PORT), NoCacheHandler)

    print(f"\n========================================================", flush=True)
    print(f" Aegis Modernize viewer is running:", flush=True)
    print(f" • Local:   http://localhost:{PORT}/#/modernize", flush=True)
    print(f" • Direct:  http://127.0.0.1:{PORT}/#/modernize", flush=True)
    print(f"========================================================\n", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")

