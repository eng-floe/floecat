#!/usr/bin/env python3
# Copyright 2026 Yellowbrick Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A Delta Sharing recipient endpoint, for smoke coverage of directory access.

The reference server implements directory access, but no published image does: the server-side
change landed in March 2026 and the last deltaio/delta-sharing-server tag is from April 2024. This
serves the four endpoints the Floecat client calls, over a fixture the harness has already written
to LocalStack, so the vend has something to be checked against locally.

Deliberately not a Delta Sharing server. It does not implement the query endpoint, paging, or url
access, and it reads no Delta log: the schema and location it answers with are handed to it by the
harness. What it exists to exercise is the recipient path -- bearer authorization, discovery, and a
temporary credential over a location -- against the real client.

Configuration, all through the environment:

  SHARING_SHARE, SHARING_SCHEMA, SHARING_TABLE  the one table it shares
  SHARING_TABLE_LOCATION                        s3:// prefix the credential is scoped to
  SHARING_SCHEMA_JSON_FILE                      file holding the table's Delta schemaString, read
                                                per request so the harness can write it after the
                                                stack is up and the fixture exists
  SHARING_BEARER_TOKEN                          the recipient token it accepts
  SHARING_VENDED_ACCESS_KEY_ID                  the triad it vends
  SHARING_VENDED_SECRET_ACCESS_KEY
  SHARING_VENDED_SESSION_TOKEN
  SHARING_ACCESS_MODES                          comma-separated, empty to send no field at all
  SHARING_STATE_LOCATION                        whether the listing and metaData action name the
                                                table's location. The reference server names it on
                                                neither, only on the credential response, so "false"
                                                reproduces the shape a real recipient meets.
  SHARING_RESPONSE_FORMAT                       delta (default) or parquet, selecting whether the
                                                metaData action nests the table's fields under
                                                deltaMetadata the way a server honouring the
                                                capability header does
  SHARING_TLS_CERT, SHARING_TLS_KEY             PEM paths; the client requires HTTPS
  SHARING_PORT                                  default 8443
"""

import json
import os
import ssl
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SHARE = os.environ.get("SHARING_SHARE", "floecat_smoke")
SCHEMA = os.environ.get("SHARING_SCHEMA", "sharing")
TABLE = os.environ.get("SHARING_TABLE", "call_center")
TABLE_LOCATION = os.environ.get("SHARING_TABLE_LOCATION", "")
SCHEMA_JSON_FILE = os.environ.get("SHARING_SCHEMA_JSON_FILE", "/etc/sharing/conf/schema.json")
BEARER_TOKEN = os.environ.get("SHARING_BEARER_TOKEN", "")
ACCESS_KEY_ID = os.environ.get("SHARING_VENDED_ACCESS_KEY_ID", "test")
SECRET_ACCESS_KEY = os.environ.get("SHARING_VENDED_SECRET_ACCESS_KEY", "test")
SESSION_TOKEN = os.environ.get("SHARING_VENDED_SESSION_TOKEN", "smoke-session-token")
CREDENTIAL_VALIDITY_SECONDS = int(os.environ.get("SHARING_CREDENTIAL_VALIDITY_SECONDS", "3600"))
PORT = int(os.environ.get("SHARING_PORT", "8443"))

# Empty means the field is omitted entirely, which is what the reference server does and what the
# client's ask-rather-than-refuse default exists for.
ACCESS_MODES = [m.strip() for m in os.environ.get("SHARING_ACCESS_MODES", "url,dir").split(",") if m.strip()]

# The client asks for responseformat=delta, so answering in that shape is what a conforming server
# does. Answering flat is the legacy shape a server ignoring the header returns, and both are worth
# being able to point the smoke at.
RESPONSE_FORMAT = os.environ.get("SHARING_RESPONSE_FORMAT", "delta").strip().lower()
STATE_LOCATION = os.environ.get("SHARING_STATE_LOCATION", "true").strip().lower() != "false"

TABLE_PREFIX = f"/shares/{SHARE}/schemas/{SCHEMA}/tables/{TABLE}"

# Every request this server answered, so a smoke run can assert the vend was actually reached
# rather than inferring it from a check that passed.
_requests_lock = threading.Lock()
_requests = []


def _record(method, path):
    with _requests_lock:
        _requests.append(f"{method} {path}")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[delta-sharing-stub] %s\n" % (fmt % args))

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        _record("GET", path)

        # Not part of the protocol. The harness reads it to prove the credential endpoint was
        # reached, and it carries no secret.
        if path == "/_smoke/requests":
            with _requests_lock:
                self._json({"requests": list(_requests)})
            return

        if not self._authorized():
            return

        if path == "/shares":
            self._json({"items": [{"name": SHARE, "id": "share-0001"}]})
        elif path == f"/shares/{SHARE}/schemas":
            self._json({"items": [{"name": SCHEMA}]})
        elif path == f"/shares/{SHARE}/schemas/{SCHEMA}/tables":
            self._json({"items": [self._table_item()]})
        elif path == f"{TABLE_PREFIX}/metadata":
            try:
                with open(SCHEMA_JSON_FILE, "r", encoding="utf-8") as handle:
                    schema_string = handle.read().strip()
            except OSError as missing:
                # Read per request rather than at startup: the schema comes off the fixture, which
                # the harness copies into place after this container is running.
                self._error(503, f"schema not yet available: {missing}")
                return
            table_fields = {
                "id": "table-0001",
                "name": TABLE,
                "format": {"provider": "parquet"},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
            }
            wrapper = {"version": 0}
            if STATE_LOCATION:
                wrapper["location"] = TABLE_LOCATION
            if ACCESS_MODES:
                wrapper["accessModes"] = ACCESS_MODES
            if RESPONSE_FORMAT == "delta":
                wrapper["deltaMetadata"] = table_fields
                protocol = {"deltaProtocol": {"minReaderVersion": 1, "readerFeatures": []}}
            else:
                wrapper.update(table_fields)
                protocol = {"minReaderVersion": 1, "readerFeatures": []}
            self._ndjson([{"protocol": protocol}, {"metaData": wrapper}])
        else:
            self._error(404, "not found")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        _record("POST", path)
        # Drained whether or not it is used: leaving it unread wedges a keep-alive connection.
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)

        if not self._authorized():
            return

        if path == f"{TABLE_PREFIX}/temporary-table-credentials":
            self._json(
                {
                    "credentials": {
                        "location": TABLE_LOCATION,
                        "expirationTime": int((time.time() + CREDENTIAL_VALIDITY_SECONDS) * 1000),
                        "awsTempCredentials": {
                            "accessKeyId": ACCESS_KEY_ID,
                            "secretAccessKey": SECRET_ACCESS_KEY,
                            "sessionToken": SESSION_TOKEN,
                        },
                    }
                }
            )
        else:
            self._error(404, "not found")

    def _table_item(self):
        item = {
            "name": TABLE,
            "share": SHARE,
            "schema": SCHEMA,
            "id": "table-0001",
            "shareId": "share-0001",
        }
        if STATE_LOCATION:
            item["location"] = TABLE_LOCATION
        if ACCESS_MODES:
            item["accessModes"] = ACCESS_MODES
        return item

    def _authorized(self):
        # 401 rather than 403: the token is absent or wrong, which is what the client reports as
        # UNAUTHENTICATED and what a smoke run asserts a wrong token produces.
        if self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
            self._error(401, "recipient token not accepted")
            return False
        return True

    def _json(self, payload):
        self._respond(200, "application/json; charset=utf-8", json.dumps(payload).encode("utf-8"))

    def _ndjson(self, actions):
        body = "\n".join(json.dumps(a) for a in actions).encode("utf-8")
        self._respond(200, "application/x-ndjson; charset=utf-8", body)

    def _error(self, status, message):
        self._respond(
            status, "application/json; charset=utf-8", json.dumps({"message": message}).encode("utf-8")
        )

    def _respond(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    cert = os.environ.get("SHARING_TLS_CERT")
    key = os.environ.get("SHARING_TLS_KEY")
    if not cert or not key:
        sys.stderr.write("[delta-sharing-stub] SHARING_TLS_CERT and SHARING_TLS_KEY are required\n")
        return 2
    if not TABLE_LOCATION:
        sys.stderr.write("[delta-sharing-stub] SHARING_TABLE_LOCATION is required\n")
        return 2

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert, keyfile=key)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    sys.stderr.write(
        "[delta-sharing-stub] sharing %s.%s.%s at %s on :%d (accessModes=%s)\n"
        % (SHARE, SCHEMA, TABLE, TABLE_LOCATION, PORT, ACCESS_MODES or "<absent>")
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
