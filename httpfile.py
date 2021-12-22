import io
import itertools
import logging

import sortedcontainers
import httpx


Client = httpx.Client | httpx.AsyncClient


logger = logging.getLogger(__name__)


class Buffer:
    def __init__(self, start: int, size: int, data: bytes | None = None):
        self.start = start
        self.size = size
        assert size >= 0
        if data is not None:
            assert len(data) == size
        self._data = data

    @property
    def data(self):
        if self._data is None:
            raise ValueError("Accessing bytes before set")
        return self._data

    @data.setter
    def data(self, value):
        assert len(value) == self.size
        self._data = value

    def __lt__(self, other):
        return type(self) == type(other) and self.start < other.start

    def __eq__(self, other):
        # TODO: think about using data for equality too
        return type(self) == type(other) and self.start == other.start

    def __repr__(self):
        return f"Buffer<start={self.start}, end={self.end}, allocated={self._data is not None}>"

    def __len__(self):
        return self.size

    @property
    def end(self):
        return self.start + len(self)

    @property
    def format_range(self):
        return f"bytes={self.start}-{self.end - 1}"


class HTTPFileIO:
    def __init__(self, session: Client | None = None):
        self.session: Client = session or httpx.Client()

    def do_read_ranges(self, url, buffers: list[Buffer]):
        for buffer in buffers:
            if buffer._data is None:
                headers = {"Range": buffer.format_range}
                logger.debug("get - %s - %s", url, headers["Range"])
                r = self.session.get(url, headers=headers)
                r.raise_for_status()
                buffer._data = r.content
                assert len(buffer._data) == len(buffer)
        return

    def do_discover_length(self, url: str) -> int:
        # TODO: handle optional
        r = self.session.head(url)
        r.raise_for_status()
        return int(r.headers["content-length"])


class FileFileIO:
    def do_read_ranges(self, buf: io.BytesIO, buffers: list[Buffer]):
        for buffer in buffers:
            if buffer._data is None:
                buf.seek(buffer.start)
                size = len(buffer)
                buffer._data = buf.read(size)
                assert len(buffer._data) == len(buffer)


class HTTPFile(io.IOBase):
    """
    A file-like interface over bytes from a remote HTTP server.
    """

    # Our internal bytes-management strategy consists of two abstractions
    # 1. sans-io: All network calls happen through an HTTPFileIO object.
    #    This class is *only* concerned with managing positions and buffers
    #    of bytes. All I/O is delegated
    # 2. A sorted collection of sorted, non-overlapping buffers, representing
    #    ranges of requested data

    # The state of this object is the *position* and its *buffers*.
    # The *position* is a non-negative integer representing where
    # in the logical file stream we're at. All read operations will
    # move this position.
    # The *buffers* are a sorted collection of bytes.

    # To illustrate point 2, consider this sequence
    # >>> f = HTTPFile(...)
    # >>> f.seek(5)  # 1: move 5 bytes - position 5
    # >>> f.read(5)  # 2: read 5 bytes - bytes 5-10
    # >>> f.read(5)  # 3: read 5 bytes - bytes 10-15
    # >>> f.seek(0)  # 4: Move to the start
    # >>> f.read(5)  # 5: Read 5 bytes from the start;
    # What's our state after each operation?#
    #
    # Stage 1: f.seek(5)
    #   position: 5
    #   buffers: []
    # Stage 2: f.read(5)
    #   position: 10
    #   buffers: [b'22222']  # 2 represents the second 5-byte chunk
    # Stage 3: f.read(5)
    #   position: 15
    #   buffers: [b'22222', b'33333']
    # Stage 4: f.seek(5)
    #   position: 0
    #   buffers: [b'22222', b'33333']
    # Stage 5: f.read(5)
    #   position: 0
    #   buffers: [b'11111', b'22222', b'33333']

    # TODO: how to handle compaction / consolidation?

    def __init__(self, url: str, session: Client | None = None):
        self.url = url
        self._buffers: sortedcontainers.SortedList = sortedcontainers.SortedList()
        self._position: int = 0
        self._content_length: int | None = None
        self._io: HTTPFileIO = HTTPFileIO(session)

    def read(self, size: int = -1) -> bytes:
        # Cases
        # 1. | start --- position --- to --- end |
        # cases:
        # 1. | start --- position --- end |
        # 2. | position --- start --- end |
        # 3. | start --- end --- position |
        if size == -1:
            end = self.content_length
        else:
            end = self._position + size

        logger.warning("read %d - %d", self._position, end)
        new_buffers = ranges_for_read(self._buffers, self._position, end)
        # if size == 512: breakpoint()
        # ---------------------
        # this must be atomic !
        # mutates buffer's data in place
        self._io.do_read_ranges(
            self.url, [buf for buf in new_buffers if buf._data is None]
        )
        self._buffers = new_buffers
        result = self._build_result(size)
        assert len(result) == size
        self._position += size
        # ---------------------
        return result

    def _build_result(self, size) -> bytes:
        start = self._position
        end = self._position + size

        start_idx = self._buffers.bisect_right(Buffer(start, 0)) - 1
        end_idx = self._buffers.bisect_right(Buffer(start + size, 0)) - 1

        result = []
        if start_idx == end_idx:
            b = self._buffers[start_idx]
            assert b.end >= (start + size)
            result.append(b.data[start - b.start : start - b.start + size])

        else:
            buf = self._buffers[start_idx]
            result.append(buf.data[start - buf.start :])

            for buf in self._buffers[start_idx + 1 : end_idx]:
                result.append(buf.data)

            buf = self._buffers[end_idx]
            if buf.end == end:
                result.append(buf.data)
            else:
                result.append(buf.data[: end - buf.end])

        # if self._buffers[start_idx].start < start:
        #     result.append(self._buffers[start_idx])

        return b"".join(result)
        # return b"".join(b.data for b in self._buffers[start_idx:end_idx])

    @property
    def content_length(self) -> int:
        if self._content_length is None:
            # TODO: cache the fact that we've looked this up. Use another sentinel
            _content_length = self._io.do_discover_length(self.url)
            if _content_length is None:
                raise ValueError(
                    "The HTTP server doesn't return the 'Content-Length' header."
                )
            self._content_length = _content_length

        return self._content_length

    def readinto(self, b):
        # TODO: think about optimizing
        result = self.read(len(b))
        b[:] = result
        return len(result)

    def seek(self, offset, whence=io.SEEK_SET):
        # TODO: not fully implemented
        if whence == io.SEEK_SET:
            # from the start of the stream.
            assert offset >= 0
            self._position = offset
        elif whence == io.SEEK_CUR:
            # from the current position
            self._position += offset
        elif whence == io.SEEK_END:
            self._position = self.content_length + offset
        else:
            raise ValueError(f"Invalid value for 'whence': {whence}")

    def seekable(self):
        return True

    def tell(self):
        return self._position


# ranges, slices, etc.
# Given:
#   1. A list of buffers
#   2. a start and end to read
# we need to construct
#   1. A list of range requests to make
#   2. A list of slices from buffers to use
# The problem: the slices are always 0-indexed,
# while the range requests always have a start / offset.
#
def pairwise(iterable):
    # New in Python 3.10
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def triplewise(iterable):
    # New in Python 3.10
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b, c = itertools.tee(iterable, 3)
    next(b, None)
    next(c, None)
    next(c, None)
    return zip(a, b, c)


def ranges_for_read(
    buffers: sortedcontainers.SortedList, start: int, end: int
) -> sortedcontainers.SortedList[Buffer]:
    """
    Given a start and end positions, find

    1. The existing buffers to read from
    2. The new ranges to request.

    Ranges are expressed as ...
    """
    size = end - start
    buffers = buffers.copy()

    if len(buffers) == 0:
        buffers.add(Buffer(start, size))
    else:
        if end <= buffers[0].start:
            # case 1: we're completely to the left of the leftmost range
            buffers.add(Buffer(start, size))
            return buffers

        if start >= buffers[-1].end:
            # case 2: we're completely to the right of the rightmost range
            buffers.add(Buffer(start, size))
            return buffers

        # dummy buffers, to see where we land
        start_idx = buffers.bisect_left(Buffer(start, 0))

        if start_idx == len(buffers):
            # we start in the last buffer
            if end < buffers[-1].end:
                return buffers
            else:
                buffers.add(Buffer(buffers[-1].end, end - buffers[-1].end))
                return buffers

        if start < buffers[start_idx].start:
            buffers.add(
                Buffer(start, min(end - start + 1, buffers[start_idx].start - start))
            )

        end_idx = buffers.bisect_left(Buffer(end, 0)) - 1
        if buffers[end_idx].end < end:
            buffers.add(Buffer(buffers[end_idx].end, end - buffers[end_idx].end))

        start_idx = buffers.bisect_left(Buffer(start, 0))
        end_idx = buffers.bisect_left(Buffer(end, 0)) - 1

        i = 0
        while not done(buffers, start, end, start_idx, end_idx):
            # fill holes
            for a, b in pairwise(buffers):
                if a.end < b.start:
                    buffers.add(Buffer(a.end, min(b.start - a.end, end - a.end)))

            start_idx = buffers.bisect_left(Buffer(start, 0))
            end_idx = buffers.bisect_left(Buffer(end, 0)) - 1
            i += 1
            if i > 100:
                raise RecursionError

    return buffers


def done(buffers, start, end, start_idx, end_idx):
    return (
        (buffers[start_idx].start <= start)
        and (end <= buffers[end_idx].end)
        and all(a.end == b.start for a, b in pairwise(buffers[start_idx : end_idx + 1]))
    )
