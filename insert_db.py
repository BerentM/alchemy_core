from sqlalchemy import create_engine, MetaData
from create_db import users, addresses

engine = create_engine('sqlite:///core.db', echo=True)
metadata = MetaData()

conn = engine.connect()

# first insert
# ins = users.insert().values(name='jack', fullname='Jack Jones')
# result = conn.execute(ins)

# second insert - parametry bezposrednio przekazane do execute
# ins = users.insert()
# conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')

# insert many - nalezy przekazac liste slownikow, sql generowany jest na
# podstawie pierwszego slownika
conn.execute(addresses.insert(), [
    {'user_id': 1, 'email_address': 'jack@yahoo.com'},
    {'user_id': 1, 'email_address': 'jack@msn.com'},
    {'user_id': 2, 'email_address': 'www@www.org'},
    {'user_id': 2, 'email_address': 'wendy@aol.com'},
]


)
