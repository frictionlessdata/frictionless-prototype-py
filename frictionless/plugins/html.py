from pyquery import PyQuery as pq
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser


# Plugin


class HtmlPlugin(Plugin):
    def create_parser(self, file):
        if file.format == "html":
            return HtmlParser(file)


# Parser


class HtmlParser(Parser):
    Dialect = property(lambda self: HtmlDialect)

    # Read

    def read_data_stream_create(self):
        dialect = self.file.dialect

        # Get Page content
        page = pq(self.loader.text_stream.read(), parser="html")

        # Find required table
        if dialect.selector:
            table = pq(page.find(dialect.selector)[0])
        else:
            table = page

        # Stream headers
        data = (
            table.children("thead").children("tr")
            + table.children("thead")
            + table.children("tr")
            + table.children("tbody").children("tr")
        )
        data = [pq(r) for r in data if len(r) > 0]
        first_row = data.pop(0)
        headers = [pq(th).text() for th in first_row.find("th,td")]
        yield headers

        # Stream data
        data = [pq(tr).find("td") for tr in data]
        data = [[pq(td).text() for td in tr] for tr in data if len(tr) > 0]
        yield from data


# Dialect


class HtmlDialect(Dialect):
    """Html dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        selector? (str): selector

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "selector": {"type": "string"},
            "headersRow": {"type": ["number", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        selector=None,
        headers_row=None,
        headers_joiner=None,
        metadata_root=None
    ):
        self.setdefined("selector", selector)
        super().__init__(
            descriptor,
            headers_row=headers_row,
            headers_joiner=headers_joiner,
            metadata_root=metadata_root,
        )

    @property
    def selector(self):
        return self.get("selector", "table")

    # Expand

    def expand(self):
        super().expand()
        self.setdetault("selector", self.selector)
