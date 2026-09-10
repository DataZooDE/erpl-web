#!/usr/bin/env python3
"""Minimal OData v4 service serving synthetic pages of a configurable size.

Usage: odata_server.py <port> <rows_per_page> <total_rows> [payload_chars]

Isolates the memory relationship from network variance: the page size is
whatever we say it is, and the row shape is fixed.
Also records whether the client sent `Prefer: odata.maxpagesize` and honours it.
"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PORT = int(sys.argv[1])
ROWS_PER_PAGE = int(sys.argv[2])
TOTAL_ROWS = int(sys.argv[3])
PAYLOAD = int(sys.argv[4]) if len(sys.argv) > 4 else 200

BASE = f"http://127.0.0.1:{PORT}/svc/"

METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices>
  <Schema Namespace="Demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
   <EntityType Name="Item">
    <Key><PropertyRef Name="Id"/></Key>
    <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
    <Property Name="Name" Type="Edm.String"/>
    <Property Name="Amount" Type="Edm.Double"/>
    <Property Name="Payload" Type="Edm.String"/>
   </EntityType>
   <EntityContainer Name="Container">
    <EntitySet Name="Items" EntityType="Demo.Item"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""

FILLER = "x" * PAYLOAD

PREFER_SEEN = []


def make_row(i):
    return {
        "Id": i,
        "Name": f"Item number {i}",
        "Amount": i * 1.5,
        "Payload": FILLER,
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # silence
        pass

    def _send(self, body, ctype):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("OData-Version", "4.0")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        prefer = self.headers.get("Prefer")
        if prefer:
            PREFER_SEEN.append(prefer)

        if u.path.rstrip("/") == "/svc":
            self._send(json.dumps({
                "@odata.context": BASE + "$metadata",
                "value": [{"name": "Items", "kind": "EntitySet", "url": "Items"}],
            }), "application/json;odata.metadata=minimal")
            return
        if u.path == "/svc/$metadata":
            self._send(METADATA, "application/xml")
            return
        if u.path == "/svc/Items":
            skip = int(q.get("$skip", ["0"])[0])
            page = ROWS_PER_PAGE
            # Honour a client-sent maxpagesize, the behaviour a compliant service has.
            if prefer and "odata.maxpagesize=" in prefer:
                try:
                    page = min(page, int(prefer.split("odata.maxpagesize=")[1].split(",")[0]))
                except ValueError:
                    pass
            end = min(skip + page, TOTAL_ROWS)
            doc = {
                "@odata.context": BASE + "$metadata#Items",
                "value": [make_row(i) for i in range(skip, end)],
            }
            if end < TOTAL_ROWS:
                doc["@odata.nextLink"] = f"{BASE}Items?$skip={end}"
            body = json.dumps(doc)
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json;odata.metadata=minimal")
            self.send_header("OData-Version", "4.0")
            if prefer and "odata.maxpagesize=" in prefer:
                self.send_header("Preference-Applied", f"odata.maxpagesize={page}")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            sys.stderr.write(f"PAGE skip={skip} rows={end - skip} bytes={len(data)} prefer={prefer}\n")
            sys.stderr.flush()
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    sys.stderr.write(f"READY port={PORT} rows_per_page={ROWS_PER_PAGE} total={TOTAL_ROWS} payload={PAYLOAD}\n")
    sys.stderr.flush()
    srv.serve_forever()
