from table import Table_column, Table_row, Row


class Supplier_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['suppkey']
        self.foreign_key_name = ['nationkey']
        self.suppkey = []
        self.name = []
        self.address = []
        self.nationkey = []
        self.phone = []
        self.acctbal = []
        self.comment = []

        super().__init__('SUPPLIER')



class Supplier(Row):
    def __init__(self, values):
        self.suppkey = None
        self.name = None
        self.address = None
        self.nationkey = None
        self.phone = None
        self.acctbal = None
        self.comment = None

        super().__init__(values)


class Supplier_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['suppkey']
        self.foreign_key_name = ['nationkey']

        super().__init__(Supplier, 'SUPPLIER')
