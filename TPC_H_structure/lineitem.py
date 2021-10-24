from table import Table_column, Table_row, Row


class Lineitem_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['orderkey', 'linenumber']
        self.foreign_key_name = ['orderkey', 'partkey', 'suppkey']
        self.orderkey = []
        self.partkey = []
        self.suppkey = []
        self.linenumber = []
        self.quantity = []
        self.extendedprice = []
        self.discount = []
        self.returnflag = []
        self.linestatus = []
        self.shipdate = []
        self.commitdate = []
        self.receiptdate = []
        self.shipinstruct = []
        self.shipmode = []
        self.comment = []

        super().__init__('LINEITEM')


class Lineitem(Row):
    def __init__(self, values):
        self.orderkey = None
        self.partkey = None
        self.suppkey = None
        self.linenumber = None
        self.quantity = None
        self.extendedprice = None
        self.discount = None
        self.returnflag = None
        self.linestatus = None
        self.shipdate = None
        self.commitdate = None
        self.receiptdate = None
        self.shipinstruct = None
        self.shipmode = None
        self.comment = None

        super().__init__(values)


class Lineitem_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['orderkey', 'linenumber']
        self.foreign_key_name = ['orderkey', 'partkey', 'suppkey']
        super().__init__(Lineitem, 'LINEITEM')
