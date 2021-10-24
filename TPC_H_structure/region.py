from table import Table_column, Table_row, Row


class Region_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['regionkey']
        self.foreign_key_name = []
        self.regionkey = []
        self.name = []
        self.comment = []

        super().__init__('NATION')


class Region(Row):
    def __init__(self, values):
        self.regionkey = None
        self.name = None
        self.comment = None

        super().__init__(values)


class Region_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['regionkey']
        self.foreign_key_name = []

        super().__init__(Region, 'REGION')
