import io
import string

import sortedcontainers
import pytest
from sortedcontainers import sortedlist
from sortedcontainers.sortedlist import SortedList
import httpfile


@pytest.mark.parametrize(
    "start, end, expected",
    [
        (0, 1, [(0, 1)]),
        (1, 5, [(1, 5)]),
    ],
)
def test_ranges_for_reads_empty(start, end, expected):
    buffers = sortedcontainers.SortedList()
    result = httpfile.ranges_for_read(buffers, start, end)
    expected = sortedcontainers.SortedList(httpfile.Buffer(start, end - start) for start, end in expected)
    
    assert result == expected

x = b"11111"
y = b"222"


@pytest.mark.parametrize(
    "start, end, expected",
    [
        (0, 1, [(0, 1, None), (5, 5, x), (12, 3, y)]),
        (0, 5, [(0, 5, None), (5, 5, x), (12, 3, y)]),
        (0, 6, [(0, 5, None), (5, 5, x), (12, 3, y)]),
        (0, 10, [(0, 5, None), (5, 5, x), (12, 3, y)]),
        (0, 11, [(0, 5, None), (5, 5, x), (10, 1, None), (12, 3, y)]),
        (0, 13, [(0, 5, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        # (0, 15, [(0, 5, None), (5, 5, x), (10, 12, None), (12, 3, y)]),
        # (0, 17, [(0, 5, None), (5, 5, x), (10, 12, None), (12, 3, y), (15, 2, None)]),

        # # non-zero start position
        # (1, 1, [(1, 1, None), (5, 5, x), (12, 3, y)]),
        # (1, 2, [(1, 2, None), (5, 5, x), (12, 3, y)]),
        # (1, 5, [(1, 5, None), (5, 5, x), (12, 3, y)]),
        # (1, 6, [(1, 5, None), (5, 5, x), (12, 3, y)]),
        # (1, 10, [(1, 5, None), (5, 5, x), (12, 3, y)]),
        # (1, 11, [(1, 5, None), (5, 5, x), (10, 1, None), (12, 3, y)]),
        # (1, 13, [(1, 5, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        # (1, 15, [(1, 5, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        # (1, 17, [(1, 5, None), (5, 5, x), (10, 2, None), (12, 3, y), (15, 2, None)]),

        # # start in the first buffer
        # (5, 6, [(5, 5, x), (12, 3, y)]),
        # (5, 10, [(5, 5, x), (12, 3, y)]),
        # (5, 11, [(5, 5, x), (12, 3, y)]),
        # (5, 11, [(5, 5, x), (10, 1, None), (12, 3, y)]),
        # (5, 11, [(False, 5, 10), (True, 10, 11)]),
        # (5, 13, [(False, 5, 10), (True, 10, 12), (False, 12, 13)]),
        # (5, 15, [(False, 5, 10), (True, 10, 12), (False, 12, 15)]),
        # (5, 17, [(False, 5, 10), (True, 10, 12), (False, 12, 15), (True, 15, 17)]),

        # start past the first buffer
        # (10, 11, [(False, 0, 0), (True, 10, 11)]),
        # (10, 13, [(False, 0, 0), (True, 10, 12), (False, 12, 13)]),
        # (10, 15, [(False, 0, 0), (True, 10, 12), (False, 12, 15)]),
        # (10, 17, [(False, 0, 0), (True, 10, 12), (False, 12, 15), (True, 15, 17)]),
    ],
)
def test_ranges_for_reads(start, end, expected):
    buffers = sortedcontainers.SortedList()
    b = httpfile.Buffer(5, 5, x)
    c = httpfile.Buffer(12, 3, y)
    buffers.update([b, c])
    buffers

    result = httpfile.ranges_for_read(buffers, start, end)
    expected = sortedcontainers.SortedList(
        httpfile.Buffer(*x) for x in expected
    )
    assert result == expected
    for a, b in zip(result, expected):
        assert a.start == b.start
        assert a.size == b.size
        if b._data is None:
            assert a._data is None



def test_matches_file():
    buf = io.BytesIO(string.ascii_lowercase.encode())
    buf.seek(0)

    file = httpfile.HTTPFile(buf)
    file._io = httpfile.FileFileIO()

    assert file.read(2) == b'ab'
    assert file._buffers == sortedcontainers.SortedList([
        httpfile.Buffer(0, 2, b"ab")
    ])

    buf.seek(0)


def test_matches_file2():
    buf = io.BytesIO(string.ascii_lowercase.encode())
    buf.seek(0)
    file = httpfile.HTTPFile(buf)
    file._io = httpfile.FileFileIO()

    file.seek(2)
    assert file.read(2) == b"cd"
    assert file._buffers == sortedcontainers.SortedList([
        httpfile.Buffer(2, b"cd")
    ])

