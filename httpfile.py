import io
import typing
import itertools

import sortedcontainers
import httpx


Client = httpx.Client | httpx.AsyncClient


class Buffer:
    def __init__(self, start: int, size: int, data: bytes | None = None):
        self.start = start
        self.size = size
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
        return f"Buffer<start={self.start}, end={self.end}>"

    def __len__(self):
        return self.size

    @property
    def end(self):
        return self.start + len(self)


class HTTPFileIO:
    def __init__(self, session: Client | None = None):
        self.session: Client = session or httpx.Client()

    def do_read_ranges(self, url, start, end):
        return

    def do_discover_length(self, url: str) -> int:
        # TODO: handle optional
        r = self.session.head(url)
        r.raise_for_status()
        return int(r.headers["content-length"])


class FileFileIO:
    def do_read_ranges(self, buf, ranges: list[Buffer]):
        results = []

        for range_read in ranges:
            buf.seek(range_read.start)
            size = range_read.end - range_read.start
            chunk = Buffer(range_read.start, size)
            chunk.data = buf.read(size)
            results.append(chunk)

        return results


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

        new_buffers = ranges_for_read(self._buffers, self._position, end)
        self._io.do_read_ranges(
            self.url, [buf for buf in new_buffers if buf._data is None]
        )
        # ---------------------
        # this must be atomic !
        self._buffers = new_buffers
        result = self._build_result()
        self._position += size
        # ---------------------
        return result

    def _build_result(self, ranges: list[Buffer]) -> bytes:
        result = []
        for buf, range_ in zip(self._buffers, ranges):
            # which is shorter, and is that OK?
            result.append(buf.data[: range_.end])
        return b"".join(result)

    @property
    def content_length(self) -> int:
        if self._content_length is None:
            # TODO: cache the fact that we've looked this up. Use another sentinel
            _content_length = self._io.do_discover_length()
            if _content_length is None:
                raise ValueError(
                    "The HTTP server doesn't return the 'Content-Length' header."
                )
            self._content_length = _content_length

        return self._content_length

    def readinto(self, b):
        ...

    def seek(self, offset, whence=io.SEEK_SET):
        # TODO: not fully implemented
        self._position += offset
        ...

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
        # if start_idx == 0:
        # two cases:
        # breakpoint()
        if end <= buffers[0].start:
            # case 1: we're completely to the left of the leftmost range
            buffers.add(Buffer(start, size))
            # print(result)
        else:
            # Now we iterate through the buffers, filling in holes with new buffers as necessary
            if start < buffers[0].start:
                buffers.add(Buffer(start, buffers[0].start - start))

            start_idx = buffers.bisect_left(
                Buffer(start, 0)
            )  # dummy buffers, to see where we land
            end_idx = buffers.bisect_left(Buffer(end, 0)) - 1

            # TODO: test / handle cases with len(buffers) < 3
            while not done(buffers, start, end, start_idx, end_idx):
                for a, b in pairwise(buffers):
                    if a.end < b.start:
                        # fill a hole
                        buffers.add(Buffer(a.end, min(b.start - a.end, end - a.end)))

                start_idx = buffers.bisect_left(
                    Buffer(start, 0)
                )  # dummy buffers, to see where we land
                end_idx = buffers.bisect_left(Buffer(end, 0)) - 1


            # # case 2: we start left of buffers, but overlap some.

            # # print(result)
            # # Now figure out what we need for the rest, based on the end_idx.
            # assert end_idx >= 1
            # # need to slurp up all the intermediate buffers, while also
            # # adding reads for "holes".
            # last_already_read = buffers[start_idx].end

            # for buf in buffers[start_idx:end_idx]:
            #     if last_already_read <= buf.start:
            #         # we have a hole to fill
            #         # result.add(Buffer(True, last_already_read, buf.start))
            #         # print(result)
            #         assert 0

            #     # result.add(Buffer(False, buf.start, min(end, buf.end)))
            #     # print(result)

            # # we might have some left over here
            # if buffers[-1].end < end:
            #     # ijalei.sjfa I don't like this
            #     # we have state in two places right now
            #     # think about alterantives.
            #     # result.add(RangeRead(True, result[-1].end, end))
            #     # print(result)
            #     assert 0

    return buffers


def done(buffers, start, end, start_idx, end_idx):
    return (
        (buffers[start_idx].start <= start)
        and (end <= buffers[end_idx].end)
        and all(a.end == b.start for a, b in pairwise(buffers[start_idx:end_idx + 1]))
    )
