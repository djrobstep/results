from pathlib import Path

from pytest import raises

from results import (
    detect_enc,
    detect_string_enc,
    dicts_from_rows,
    files,
    from_file,
    from_files,
    smart_open,
)

FIXTURES = Path("tests/FIXTURES")


def test_detect_enc():
    path = "tests/FIXTURES/datafiles/latin1.txt"
    p = Path(path)

    assert detect_enc(p.open("rb")) == "ISO-8859-1"
    assert detect_string_enc(p.read_bytes()) == "ISO-8859-1"


def test_smart_open():
    path = "tests/FIXTURES/datafiles/messy.csv"
    p = Path(path)
    t = p.read_text()

    f = smart_open(path)
    assert f.read() == t

    f = smart_open(p.open())
    assert f.read() == t


def test_dicts_from_rows():
    assert dicts_from_rows([]) == []
    assert dicts_from_rows([("a", "b"), (1, 2)]) == [{"a": 1, "b": 2}]


def test_fileutil():
    flist = files(FIXTURES / "datafiles", extensions=[".xlsx", ".csv"])
    by_file = from_files(flist)
    assert list(by_file) == [
        Path("tests/FIXTURES/datafiles/messy.csv"),
        Path("tests/FIXTURES/datafiles/multisheet.xlsx::Sheet1"),
        Path("tests/FIXTURES/datafiles/multisheet.xlsx::Sheet2"),
        Path("tests/FIXTURES/datafiles/x.csv"),
        Path("tests/FIXTURES/datafiles/x.xlsx::Sheet1"),
    ]


def test_open_all():
    flist = files(FIXTURES / "datafiles")

    with raises(ValueError):
        from_files(flist, ignore_unopenable=False)

    from_files(flist)


def test_xlsx_readwrite(tmpdir):
    csvresults = from_file("tests/FIXTURES/datafiles/x.csv")

    dest = str(tmpdir / "out.xlsx")
    csvresults.save_xlsx(dest)
    xlsxresults = from_file(dest)

    assert csvresults == xlsxresults["Sheet1"]
