from pyquery import PyQuery as pq
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser


# Plugin


class HtmlPlugin(Plugin):
    def create_parser(self, file):
        if file.format == 'html':
            return HtmlParser(file)


# Parser


class HtmlParser(Parser):
    Dialect = property(lambda self: HtmlDialect)

    # Read

    def read_data_stream_create(self):
        dialect = self.file.dialect

        # Get Page content
        page = pq(self.loader.text_stream.read(), parser='html')

        # Find required table
        if dialect.selector:
            table = pq(page.find(dialect.selector)[0])
        else:
            table = page

        # Extract headers
        rows = (
            table.children('thead').children('tr')
            + table.children('thead')
            + table.children('tr')
            + table.children('tbody').children('tr')
        )
        rows = [pq(r) for r in rows if len(r) > 0]
        first_row = rows.pop(0)
        headers = [pq(th).text() for th in first_row.find('th,td')]

        # Extract rows
        rows = [pq(tr).find('td') for tr in rows]
        rows = [[pq(td).text() for td in tr] for tr in rows if len(tr) > 0]

        # Yield rows
        for row_number, row in enumerate(rows, start=1):
            yield (row_number, headers, row)


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
        'type': 'object',
        'additionalProperties': False,
        'properties': {'selector': {'type': 'string'}},
    }

    def __init__(self, descriptor=None, *, selector=None, metadata_root=None):
        self.setdefined('selector', selector)
        super().__init__(descriptor, metadata_root=metadata_root)

    @property
    def selector(self):
        return self.get('selector')
