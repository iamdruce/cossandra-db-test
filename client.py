from cassandra.auth import PlainTextAuthProvider
import auth as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from tkinter import *
import tksheet

class Client:
    cols = ['item_id', 'title', 'price', 'amount']

    def __init__(self, cluster, session):
        self.cluster = cluster
        self.session = session
        self.window = Tk()

        self.window.geometry("850x525")
        self.window.protocol("WM_DELETE_WINDOW", self.on_close)
        self.window.title("Azure DB Client")

        self.window.grid_columnconfigure(0, weight = 1)
        self.sheet = tksheet.Sheet(self.window, width = 850, height = 450)
        self.sheet.grid(row=1)
        self.sheet.headers(['Item ID', 'Title', 'Price', 'Amount'])

        label_frame = LabelFrame(self.window, text='Add new item')
        # label_frame.pack()
        label_frame.grid(row=2)

        cols = []
        for j in range(4):
            e = Entry(label_frame, relief=RIDGE)
            e.grid(row=1, column=j, sticky=NSEW)
            cols.append(e)
        
        Button(label_frame, text='Add', command=lambda: self.add([col.get() for col in cols])).grid(row=1, column=4)

    def on_close(self):
        self.cluster.shutdown()
        self.window.destroy()
    
    def get_all(self):
        self.get('SELECT * FROM shop.product')
        self.draw_table()
    
    def get(self, query):
        rows = session.execute(query)

        data = []
        for row in rows:
            data.append({"item_id": row.item_id, "title": row.title, "price": row.price, "amount": row.amount})
        
        self.rows = data
    
    def delete(self, item_ids):
        if len(item_ids) > 0:
            session.execute(f"DELETE FROM shop.product WHERE item_id IN ({', '.join([str(id) for id in item_ids])})")
    
    def update(self, item_id, column, value):
        if column == 'title':
            query = f"'{value}'"
        else:
            query = f"{value}"
        session.execute(f"UPDATE shop.product SET {column} = {query} WHERE item_id = {item_id}")
    
    def add(self, data):
        item_id = data[0]
        title = data[1]
        price = data[2]
        amount = data[3]

        session.execute(f"INSERT INTO shop.product (item_id, title, price, amount) VALUES ({item_id}, '{title}', {price}, {amount})")
        self.get_all()

    def handle_edit(self, args):
        _id = args[0]
        action = args[2]
        col = args[1]
        value = args[3]

        if not action == 'Return':
            return
        
        if col == 0:
            return
        
        c = 0
        item_id = None
    
        for row in self.rows:
            if _id == c:
                item_id = row['item_id']
                break
            c += 1
        
        if item_id is None:
            return
        
        self.update(item_id, self.cols[col], value)
    
    def handle_delete_rows(self, args):
        ids = args[1]
        item_ids = []
        c = 0
        
        for row in self.rows:
            if c in ids:
                item_ids.append(row['item_id'])
            c += 1
        
        self.delete(item_ids)
    
    def draw_table(self):
        data = []
        for row in self.rows:
            data.append([row['item_id'], row['title'], row['price'], row['amount']])
        
        self.sheet.set_sheet_data(data)

        self.sheet.enable_bindings(("single_select",
            "row_select",
            "column_width_resize",
            "arrowkeys",
            "right_click_popup_menu",
            "rc_select",
            "rc_delete_row",
            "copy",
            "cut",
            "paste",
            "delete",
            "undo",
            "edit_cell"))
        
        self.sheet.extra_bindings([("end_edit_cell", self.handle_edit)])
        self.sheet.extra_bindings([("end_delete_rows", self.handle_delete_rows)])


def init_cluster():
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE
    auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
    cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
    return cluster

if __name__ == "__main__":
    cluster = init_cluster()
    session = cluster.connect()

    client = Client(cluster, session)

    client.get_all()

    client.window.mainloop()