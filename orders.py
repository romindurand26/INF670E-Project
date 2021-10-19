from table import Table_column, Table_row, Row


class Orders_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['orderkey']
        self.foreign_key_name = ['custkey']
        self.orderkey = []
        self.custkey = []
        self.orderstatus = []
        self.totalprice = []
        self.orderdate = []
        self.order_priority = []
        self.clerk = []
        self.ship_priority = []
        self.comment = []

        super().__init__('ORDERS')


class Orders(Row):
    def __init__(self, values):
        self.orderkey = None
        self.custkey = None
        self.orderstatus = None
        self.totalprice = None
        self.orderdate = None
        self.order_priority = None
        self.clerk = None
        self.ship_priority = None
        self.comment = None

        super().__init__(values)


class Orders_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['orderkey']
        self.foreign_key_name = ['custkey']
        super().__init__(Customer, 'ORDERS')