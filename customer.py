from table import Table_column, Table_row, Row


class Customer_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['custkey']
        self.foreign_key_name = ['nationkey']
        self.custkey = []
        self.name = []
        self.address = []
        self.nationkey = []
        self.phone = []
        self.acctbal = []
        self.mktsegment = []
        self.comment = []

        super().__init__('CUSTOMER')


class Customer(Row):
    def __init__(self, values):
        self.custkey = None
        self.name = None
        self.address = None
        self.nationkey = None
        self.phone = None
        self.acctbal = None
        self.mktsegment = None
        self.comment = None

        super().__init__(values)


class Customer_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['custkey']
        self.foreign_key_name = ['nationkey']
        super().__init__(Customer, 'CUSTOMER')
