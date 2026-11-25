"""The body of POST /import: multipart/form-data with exactly one part, the file, streamed straight into one spool
file as it arrives (no second copy, nothing buffered beyond one read), bounded by MAX_FILE.

The listener (tgaud.web) hands this route the connection unread, with a deadline for the whole upload, after the
app's guards (proxy token, method, same origin) passed; every other route keeps its 64 KiB form limit.
"""

from __future__ import annotations

import re
from typing import BinaryIO, Protocol

MAX_FILE = 32 * 1024 * 1024  # the largest tgstats.db the page takes
MAX_OVERHEAD = 16 * 1024  # the boundary lines and the part's headers around it
MAX_BODY = MAX_FILE + MAX_OVERHEAD
FIELD = "file"
READ = 64 * 1024  # bytes per read of the body
MAX_HEADERS = 8 * 1024  # the part's header block
_BOUNDARY = re.compile(r"""boundary=(?:"([^"]{1,70})"|([^;\s"]{1,70}))""", re.I)
_NAME = re.compile(r"""(?:^|;)\s*name="([^"]*)\"""", re.I)


class UploadError(Exception):
    """The upload is refused with this status and message."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class Readable(Protocol):
    def read(self, n: int = -1) -> bytes: ...


def boundary_of(content_type: str | None) -> bytes:
    """The boundary of a multipart/form-data Content-Type (RFC 2046: 1-70 characters)."""
    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    if ctype != "multipart/form-data":
        raise UploadError(400, "the upload must be sent as multipart/form-data (the page's form does that)")
    m = _BOUNDARY.search(content_type or "")
    if not m:
        raise UploadError(400, "the multipart upload has no boundary")
    return (m.group(1) or m.group(2)).encode("latin-1")


class _Body:
    """Exactly `length` bytes of the request body, read in pieces; EOF before them is a cut-short upload."""

    def __init__(self, stream: Readable, length: int) -> None:
        self.stream = stream
        self.left = length
        self.buf = b""

    def fill(self, want: int) -> bool:
        """Read until the buffer holds `want` bytes or the body ends; False when it ended first."""
        while len(self.buf) < want:
            if self.left <= 0:
                return False
            chunk = self.stream.read(min(READ, self.left))
            if not chunk:
                raise UploadError(400, "the upload ended before its declared length; nothing was imported")
            self.left -= len(chunk)
            self.buf += chunk
        return True

    def more(self) -> bool:
        """Append the next read to the buffer; False when the body is all read."""
        if self.left <= 0:
            return False
        return self.fill(len(self.buf) + 1)


def receive(stream: Readable, length: int, boundary: bytes, out: BinaryIO) -> int:
    """Read the `length` bytes of a multipart body from `stream` and write its one file part to `out`; returns the
    file's size. UploadError (400, 413) for anything but one part named `file` within MAX_FILE."""
    if length > MAX_BODY:
        raise UploadError(413, f"the upload is larger than {MAX_FILE // (1024 * 1024)} MiB")
    body = _Body(stream, length)
    first = b"--" + boundary + b"\r\n"
    if not body.fill(len(first)) or not body.buf.startswith(first):
        raise UploadError(400, "the upload is not a multipart form")
    body.buf = body.buf[len(first):]
    while b"\r\n\r\n" not in body.buf:
        if len(body.buf) > MAX_HEADERS or not body.more():
            raise UploadError(400, "the upload's part headers are malformed")
    head, _, body.buf = body.buf.partition(b"\r\n\r\n")
    disposition = ""
    for line in head.split(b"\r\n"):
        name, _, value = line.decode("latin-1").partition(":")
        if name.strip().lower() == "content-disposition":
            disposition = value.strip()
    m = _NAME.search(disposition)
    if not disposition.lower().startswith("form-data") or not m or m.group(1) != FIELD:
        raise UploadError(400, f"the upload must have one part: the form's file field ({FIELD})")
    delimiter = b"\r\n--" + boundary
    size = 0
    while True:
        at = body.buf.find(delimiter)
        if at >= 0:
            data, body.buf = body.buf[:at], body.buf[at + len(delimiter):]
        else:
            keep = len(delimiter) - 1  # a delimiter may begin in the last bytes: wait for the next read
            data, body.buf = body.buf[:-keep] if len(body.buf) > keep else b"", body.buf[-keep:]
        size += len(data)
        if size > MAX_FILE:
            raise UploadError(413, f"the file is larger than {MAX_FILE // (1024 * 1024)} MiB")
        if data:
            out.write(data)
        if at >= 0:
            break
        if not body.more():
            raise UploadError(400, "the upload ended inside the file: it is not a whole multipart form")
    body.fill(2)
    if not body.buf.startswith(b"--"):
        raise UploadError(400, f"the upload must have one part only: the form's file field ({FIELD})")
    while body.more():  # the epilogue (a CRLF from browsers): read and dropped
        body.buf = b""
    return size
