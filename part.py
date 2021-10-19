from table import Table_column, Table_row, Row


class Part_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['partkey']
        self.foreign_key_name = []
        self.partkey = []
        self.name = []
        self.brand = []
        self.type = []
        self.size = []
        self.container = []
        self.retailprice = []
        self.comment = []

        super().__init__('PART')



class Part(Row):
    def __init__(self, values):
        self.partkey = None
        self.name = None
        self.brand = None
        self.type = None
        self.size = None
        self.container = None
        self.retailprice = None
        self.comment = None

        super().__init__(values)


class Part_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['partkey']
        self.foreign_key_name = []

        super().__init__(Part, 'PART')
