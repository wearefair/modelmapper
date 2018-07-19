import os
import csv
import xlrd
from io import StringIO
from xml.sax import saxutils
from xml.sax import parseString

from modelmapper.misc import cached_property, DefaultList


def excel_file_to_csv_files(path, sheet_names=None):
    dirpath, basename = os.path.split(path)
    basename = basename.split('.')[0]
    with open(path, 'rb') as the_file:
        file_contents = the_file.read()
        results = excel_contents_to_csvs(file_contents, sheet_names=sheet_names)
        for sheet_name, csv_file in results.items():
            result_content = results[sheet_name].read()
            new_file_name = f'{basename}__{sheet_name}.csv'
            new_file_name = os.path.join(dirpath, new_file_name)
            with open(new_file_name, 'w') as the_file:
                the_file.write(result_content)
            print(f'exported {new_file_name}')


def excel_contents_to_csvs(file_contents, sheet_names=None):
    """
    Convert Excel file content into csvs.
    Each sheet is converted to a separate file object.
    If sheet_names is provided, only those sheet names will be converted, otherwise all.
    """
    try:
        csvs = _xls_contents_to_csvs(file_contents, sheet_names)
    except xlrd.biffh.XLRDError:
        try:
            csvs = _xls_xml_contents_to_csvs(file_contents, sheet_names)
        except Exception as e:
            csvs = None
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
        self.cell_index = None
        self.style_id = None
        self.styles = {}

    def characters(self, content):
        self.chars.append(content)

    def startElement(self, name, atts):
        if name == "Cell":
            self.chars = []
            try:
                self.cell_index = int(atts.getValue(name='ss:Index')) - 1  # indexes start from 1 in xls_xml
            except KeyError:
                self.cell_index = None
            try:
                self.style_id = atts.getValue(name='ss:StyleID')
            except KeyError:
                self.style_id = None
        elif name == "Row":
            self.cells = DefaultList()
        elif name == "Table":
            self.rows = []
        elif name == "Worksheet":
            self.worksheets.append(atts.getValue(name='ss:Name'))
        elif name == "Style":
            self.style_id = atts.getValue('ss:ID')
        elif name == "NumberFormat":
            try:
                self.styles[self.style_id] = atts.getValue('ss:Format')
            except KeyError:
                pass

    def endElement(self, name):
        if name == "Cell":
            value = ''.join(self.chars)
            if self.style_id:
                style = self.styles[self.style_id]
                if style == 'Percent':
                    try:
                        value = float(value) * 100.0
                        value = str(float(format(value, '.15f')))
                    except ValueError:
                        pass
                    value += '%'
                elif style == 'Short Date':
                    value = value.replace('T00:00:00.000', '')
            if self.cell_index:
                self.cells[self.cell_index] = value
            else:
                self.cells.append(value)
        elif name == "Row":
            self.rows.append(self.cells)
        elif name == "Table":
            self.tables.append(self.rows)

    @cached_property
    def data(self):
        return dict(zip(self.worksheets, self.tables))
