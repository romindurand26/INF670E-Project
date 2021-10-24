from table import Table_column, Table_row, Row


class Nation_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['nationkey']
        self.foreign_key_name = ['regionkey']
        self.nationkey = []
        self.name = []
        self.regionkey = []
        self.comment = []

        super().__init__(Nation_row, 'NATION')


class Nation(Row):
    def __init__(self, values):
        self.nationkey = None
        self.name = None
        self.regionkey = None
        self.comment = None

        super().__init__(values)


class Nation_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['nationkey']
        self.foreign_key_name = ['regionkey']

        super().__init__(Nation_column, Nation, 'NATION')

n = Nation_row()
n.add([45, 'France', 5, 'hhygyguut'])
n.dump_row()