from urllib.parse import _coerce_args, urlsplit

NETLOC_PARTS = ["username", "password", "hostname", "port"]

SCHEMES_USING_NETLOC = [
    "",
    "ftp",
    "http",
    "gopher",
    "nntp",
    "telnet",
    "imap",
    "wais",
    "file",
    "mms",
    "https",
    "shttp",
    "snews",
    "prospero",
    "rtsp",
    "rtspu",
    "rsync",
    "svn",
    "svn+ssh",
    "sftp",
    "nfs",
    "git",
    "git+ssh",
    "ws",
    "wss",
]


def urlunsplit(components):
    """Combine the elements of a tuple as returned by urlsplit() into a
    complete URL as a string. The data argument can be any five-item iterable.
    This may result in a slightly different, but equivalent URL, if the URL that
    was parsed originally had unnecessary delimiters (for example, a ? with an
    empty query; the RFC states that these are equivalent)."""
    scheme, netloc, url, query, fragment, _coerce_result = _coerce_args(*components)

    if netloc or (scheme and url[:2] != "//"):
        if url and url[:1] != "/":
            url = "/" + url
        url = "//" + (netloc or "") + url
    if scheme:
        url = scheme + ":" + url
    if query:
        url = url + "?" + query
    if fragment:
        url = url + "#" + fragment
    return _coerce_result(url)


def combine_netloc_parts(parts):
    """Combine netloc parts into a netloc string."""
    username = parts["username"]
    password = parts["password"]
    hostname = parts["hostname"]
    port = parts["port"]

    login = username or ""

    if password:
        login = f"{login}:{password}"

    host_port = hostname or ""

    if host_port:
        if port:
            host_port = f"{host_port}:{port}"

    if login:
        host_port = f"{login}@{host_port}"

    return host_port


class URL:
    """A URL class that allows easy manipulation of URL components."""

    def __init__(self, url_string):
        self.s = url_string
        self.parts = self.urlsplit()

    def urlsplit(self):
        """Split the URL into components."""
        return urlsplit(self.s)

    def __getattr__(self, name):
        if hasattr(self.parts, name):
            return getattr(self.parts, name)

        if hasattr(self, name):
            return getattr(self, name)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def netloc_parts(self):
        """Get netloc parts as a dictionary."""
        return {key: getattr(self.parts, key) for key in NETLOC_PARTS}

    def replace_netloc_part(self, name, value):
        """Replace a specific netloc part."""
        parts = self.netloc_parts()
        parts[name] = value
        combined = combine_netloc_parts(parts)
        self.replace_url_part("netloc", combined)

    def replace_url_part(self, name, value):
        """Replace a specific URL part."""
        self.parts = self.parts._replace(**{name: value})
        self.s = urlunsplit(self.parts)

    def __setattr__(self, name, value):
        if name in ["s", "parts"]:
            super().__setattr__(name, value)
            return

        if name in NETLOC_PARTS:
            self.replace_netloc_part(name, value)
        elif name in self.parts._fields:
            self.replace_url_part(name, value)
        else:
            super().__setattr__(name, value)

    @property
    def absolute_path(self):
        """Get the absolute path (with leading slash)."""
        path = self.path
        if not path.startswith("/"):
            path = "/" + self.path
        return path

    @property
    def relative_path(self):
        """Get the relative path (without leading slash)."""
        return self.path.lstrip("/")

    def __str__(self):
        return urlunsplit(self.parts)


def url(url_string):
    """Create a URL object from a string."""
    return URL(url_string)
