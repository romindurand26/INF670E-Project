from table import Table_column, Table_row, Row


class PartSupp_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['partkey', 'suppkey']
        self.foreign_key_name = ['partkey', 'suppkey']
        self.partkey = []
        self.suppkey = []
        self.availqty = []
        self.supplycost = []
        self.comment = []

        super().__init__('PART')


class PartSupp(Row):
    def __init__(self, values):
        self.partkey = None
        self.suppkey = None
        self.availqty = None
        self.supplycost = None
        self.comment = None

        super().__init__(values)


class PartSupp_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['partkey', 'suppkey']
        self.foreign_key_name = ['partkey', 'suppkey']

        super().__init__(PartSupp, 'PARTSUPP')
