from sqlalchemy import Table, MetaData, create_engine, select

user = ''
pwd = ''
db_addres = ''
db_name = ''

meta = MetaData()
engine = create_engine('postgresql://{}:{}@{}/{}'.format(user, pwd, db_addres, db_name))

crimes = Table('crimes', meta, autoload=True, autoload_with=engine)

conn = engine.connect()

# dostep w stylu krotek
s = select([crimes])
result = conn.execute(s)

for row in result:
    print(row)
