from io import StringIO
import csv
import xlrd
from xml.sax import saxutils
from xml.sax import parseString

from .misc import cached_property


def excel_contents_to_csvs(file_contents, sheet_names=None):
    try:
        csvs = _xls_contents_to_csvs(file_contents, sheet_names)
    except xlrd.biffh.XLRDError:
        csvs = _xls_xml_contents_to_csvs(file_contents, sheet_names)
    return csvs


def _xls_contents_to_csvs(file_contents, sheet_names=None):
    """
    Convert Excel's old format, xls content, into csv file objects.
    Each sheet is converted to a separate file object.
    If sheet_names is provided, only those sheet names will be converted, otherwise all.
    """
    workbook = xlrd.open_workbook(file_contents=file_contents)
    sheet_names = workbook.sheet_names() if sheet_names is None else sheet_names
    result = {}
    for worksheet_name in sheet_names:
        worksheet = workbook.sheet_by_name(worksheet_name)
        result[worksheet_name] = csv_file = StringIO()
        wr = csv.writer(csv_file, lineterminator='\n')
        for rownum in range(worksheet.nrows):
            wr.writerow([str(entry) for entry in worksheet.row_values(rownum)])
        csv_file.seek(0)
    return result


def _xls_xml_contents_to_csvs(file_contents, sheet_names=None):
    data = _xls_xml_contents_to_dict(file_contents)
    sheet_names = data.keys() if sheet_names is None else sheet_names
    result = {}
    for sheet_name in sheet_names:
        result[sheet_name] = csv_file = StringIO()
        wr = csv.writer(csv_file, lineterminator='\n')
        worksheet = data[sheet_name]
        for row in worksheet:
            wr.writerow(row)
        csv_file.seek(0)
    return result


def _xls_xml_contents_to_dict(file_contents):
    """
    Convert Excel 2004 XML into csv
    """
    excelHandler = _XMLExcelHandler()
    parseString(file_contents, excelHandler)
    return excelHandler.data


class _XMLExcelHandler(saxutils.handler.ContentHandler):
    def __init__(self):
        self.chars = []
        self.cells = []
        self.rows = []
        self.tables = []
        self.worksheets = []

    def characters(self, content):
        self.chars.append(content)

    def startElement(self, name, atts):
        if name == "Cell":
            self.chars = []
        elif name == "Row":
            self.cells = []
        elif name == "Table":
            self.rows = []
        elif name == "Worksheet":
            self.worksheets.append(atts.getValue(name='ss:Name'))

    def endElement(self, name):
        if name == "Cell":
            self.cells.append(''.join(self.chars))
        elif name == "Row":
            self.rows.append(self.cells)
        elif name == "Table":
            self.tables.append(self.rows)

    @cached_property
    def data(self):
        return dict(zip(self.worksheets, self.tables))
