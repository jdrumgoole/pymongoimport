from openpyxl import load_workbook,workbook, worksheet


class CensusWorkbook:

    def __init__(self, excel_filename):
        self._workbook = load_workbook(filename=excel_filename, read_only=True)
        self._sheets = {}
        for name in self._workbook.sheetnames:
            self._sheets[name] = self._workbook[name]

    @property
    def workbook(self):
        return self._workbook

    @property
    def sheet_names(self):
        return list(self._sheets.keys())

    @property
    def sheets(self):
        return list(self._sheets.itervalues())

    def sheet(self, name):
        return self._sheets[name]

class CensusSheet:

    # def get_column_values(self, sheet,value_dict, column=2):
    #     response_values = {}
    #     for k, index in enumerate(value_dict.keys(), 16):
    #         value_dict[k] = sheet.cell(row=index, column=column).value
    #     for i in range(16, 16 + self.question_count() * 2, 2):
    #         response_values[(sheet.cell(row=i, column=column).value).values
    #     return response_values

    def __init__(self, workbook, name="None"):
        self._question_id = name
        self._workbook = workbook
        if self._question_id.startswith("Q"):
            self._sheet = self._workbook[self._question_id]
            self._responses = list(self.column_values(2))

    def sheet_name(self):
        return self._sheet.name

    @property
    def question_id(self):
        return self._question_id

    @property
    def question(self):
        return self._sheet.cell(row=10, column=1).value

    @property
    def question_count(self):
        return 0

    def response_offset(self, count=1):
            return 16 + ( 2 * count - 1)

    def column_value(self, question_number, column=1):
        row_offset = 16 + ( 2 * (question_number - 1))
        return self._sheet.cell(row=row_offset,column=column).value

    def column_values(self, column=1):
        for i in range(1, self.question_count + 1):
            yield self.column_value(i, column)

    def response_doc(self, questions, column_index):
        return dict(list(zip(questions, self.column_values(column_index))))

    @property
    def responses(self):
        return self._responses

class QuestionSheet(CensusSheet):
    def __init__(self, workbook):
        super().__init__(workbook, type(self).__name__)

class Q1(QuestionSheet):

    def __init__(self, workbook):
        super().__init__(workbook)

    @property
    def question_count(self):
        return 5


class Q2(QuestionSheet):

    def __init__(self, workbook):
        super().__init__(workbook)

    @property
    def question_count(self):
        return 6

class Q3(QuestionSheet):

    def __init__(self, workbook):
        super().__init__(workbook)

    @property
    def question_count(self):
        return 5

class Q4(QuestionSheet):

    def __init__(self, workbook):
        super().__init__(workbook)

    @property
    def question_count(self):
        return 5
if __name__ == "__main__":

    cb = CensusWorkbook("emeadevit.xlsx")
    print(cb.sheet_names)
    q3 = Q3(cb.workbook)
    print(q3.question)
    print(q3.responses)
    print(q3.response_doc(q3.responses, 4))
