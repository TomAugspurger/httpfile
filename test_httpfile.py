import io
import string

import sortedcontainers
import httpx
import pytest
from sortedcontainers import sortedlist
from sortedcontainers.sortedlist import SortedList
from werkzeug.wrappers import Request, Response
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
    expected = sortedcontainers.SortedList(
        httpfile.Buffer(start, end - start) for start, end in expected
    )

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
        (0, 15, [(0, 5, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        (0, 17, [(0, 5, None), (5, 5, x), (10, 2, None), (12, 3, y), (15, 2, None)]),
        # # non-zero start position
        # TODO: determine if reading 0 bytes is allowed
        # (1, 1, [(1, 1, None), (5, 5, x), (12, 3, y)]),
        (1, 2, [(1, 1, None), (5, 5, x), (12, 3, y)]),
        (1, 5, [(1, 4, None), (5, 5, x), (12, 3, y)]),
        (1, 6, [(1, 4, None), (5, 5, x), (12, 3, y)]),
        (1, 10, [(1, 4, None), (5, 5, x), (12, 3, y)]),
        (1, 11, [(1, 4, None), (5, 5, x), (10, 1, None), (12, 3, y)]),
        (1, 13, [(1, 4, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        (1, 15, [(1, 4, None), (5, 5, x), (10, 2, None), (12, 3, y)]),
        (1, 17, [(1, 4, None), (5, 5, x), (10, 2, None), (12, 3, y), (15, 2, None)]),
        # # start in the first buffer
        (5, 6, [(5, 5, x), (12, 3, y)]),
        (5, 10, [(5, 5, x), (12, 3, y)]),
        (5, 11, [(5, 5, x), (10, 1, None), (12, 3, y)]),
        (5, 13, [(5, 5, x), (10, 2, None), (12, 3, y)]),
        (5, 15, [(5, 5, x), (10, 2, None), (12, 3, y)]),
        (5, 17, [(5, 5, x), (10, 2, None), (12, 3, y), (15, 2, None)]),
        # start past the first buffer
        (10, 10, [(5, 5, x), (10, 1, None), (12, 3, y)]),
        (10, 11, [(5, 5, x), (10, 2, None), (12, 3, y)]),
        (10, 13, [(5, 5, x), (10, 2, None), (12, 3, y)]),
        (10, 15, [(5, 5, x), (10, 2, None), (12, 3, y)]),
        (10, 17, [(5, 5, x), (10, 2, None), (12, 3, y), (15, 2, None)]),
        # start past the last buffer
        (17, 20, [(5, 5, x), (12, 3, y), (17, 3, None)]),
        # start in the last buffer
        (13, 14, [(5, 5, x), (12, 3, y)]),
        # start *at* the last buffer
        (14, 20, [(5, 5, x), (12, 3, y), (15, 5, None)]),
    ],
)
def test_ranges_for_reads(start, end, expected):
    buffers = sortedcontainers.SortedList()
    b = httpfile.Buffer(5, 5, x)
    c = httpfile.Buffer(12, 3, y)
    buffers.update([b, c])
    buffers

    result = httpfile.ranges_for_read(buffers, start, end)
    expected = sortedcontainers.SortedList(httpfile.Buffer(*x) for x in expected)
    assert result == expected
    for a, b in zip(result, expected):
        assert a.start == b.start
        assert a.size == b.size
        if b._data is None:
            assert a._data is None


def test_build_result():
    f = httpfile.HTTPFile("")
    f._buffers = sortedcontainers.SortedList([httpfile.Buffer(5, 5, b"56789")])
    f.seek(5)
    assert f._build_result(5) == b"56789"
    assert f._build_result(2) == b"56"

    f.seek(7)
    assert f._build_result(3) == b"789"
    assert f._build_result(2) == b"78"


def test_build_result_multi():
    f = httpfile.HTTPFile("")
    f._buffers = sortedcontainers.SortedList(
        [
            httpfile.Buffer(0, 5, b"abcde"),
            httpfile.Buffer(5, 5, b"fghij"),
            httpfile.Buffer(10, 5, b"klmno"),
        ]
    )
    f.seek(0)
    assert f._build_result(15) == b"abcdefghijklmno"
    f.seek(0)
    assert f._build_result(14) == b"abcdefghijklmn"

    f.seek(0)
    assert f._build_result(10) == b"abcdefghij"

    f.seek(1)
    assert f._build_result(14) == b"bcdefghijklmno"

def test_build_result_2():
    f = httpfile.HTTPFile("")
    f._buffers = sortedcontainers.SortedList([
        httpfile.Buffer(start=0, size=8, data=b"abcdefgh"),
        httpfile.Buffer(start=8, size=8, data=b"ijklmnop"),
    ])
    f.seek(0)
    result = f._build_result(16)
    assert result == b"abcdefghijklmnop"


def test_matches_file():
    buf = io.BytesIO(string.ascii_lowercase.encode())
    buf.seek(0)

    file = httpfile.HTTPFile(buf)
    file._io = httpfile.FileFileIO()

    assert file.read(2) == b"ab"
    assert file._buffers == sortedcontainers.SortedList([httpfile.Buffer(0, 2, b"ab")])

    buf.seek(0)


def handler(request: Request):
    data = string.ascii_lowercase.encode()
    range = request.headers["Range"]
    start, end = map(int, range.split("=")[1].split("-"))

    print("request", request)
    return Response(data[start:end])
    # assert 0


def test_matches_file2():
    buf = io.BytesIO(string.ascii_lowercase.encode())
    buf.seek(0)
    file = httpfile.HTTPFile(buf)
    file._io = httpfile.FileFileIO()

    file.seek(2)
    assert file.read(2) == b"cd"
    assert file._buffers == sortedcontainers.SortedList([httpfile.Buffer(2, 2, b"cd")])


def test_it(httpserver):
    httpserver.expect_request("/file").respond_with_handler(handler)

    r = httpx.get(httpserver.url_for("/file"), headers={"Range": "bytes=0-5"})
    r.raise_for_status()
    assert r.content == b"abcde"
