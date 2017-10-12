from sqlalchemy import create_engine, func, desc, text, String
from sqlalchemy.sql import select, and_, or_, not_, bindparam
from create_db import users, addresses

engine = create_engine('sqlite:///core.db', echo=True)
conn = engine.connect()

# dostep w stylu krotek
s = select([users])
result = conn.execute(s)

for row in result:
    print(row)

# dostep slownikowy
result = conn.execute(s)
row = result.fetchone()
print("imie", row['name'], "; imie i nazwisko:", row['fullname'])

# dostep bezposrednio do kolumn
for row in conn.execute(s):
    print("imie:", row[users.c.name], "; fullname:", row[users.c.fullname])

# zakonczenie polaczenia
result.close()

# wyciaganie okreslonych kolumn w stylu krotek
s = select([users.c.name, users.c.fullname])
result = conn.execute(s)
for row in result:
    print(row)

# klauzula WHERE - uzyta dla JOINA
s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)

# and, or i not
print(and_(
    users.c.name.like('j%'),
    users.c.id == addresses.c.user_id,
    or_(
        addresses.c.email_address == 'wendy@aol.com',
        addresses.c.email_address == 'jack@yahoo.com'
    ),
    not_(
        users.c.id > 5
    )
))

# nadawanie labelek kolumnom
s = select(
    [(users.c.fullname + ", " + addresses.c.email_address).label('title')]).where(
    and_(
        users.c.id == addresses.c.user_id,
        users.c.name.between('m', 'z'),
        or_(
            addresses.c.email_address.like('%@aol.com'),
            addresses.c.email_address.like('%@msn%')
        )
    )
)
print(conn.execute(s).fetchall())

# funkcje sql
stmt = select([
    addresses.c.user_id,
    func.count(addresses.c.id).label('num_addresses')
]).order_by(desc("num_addresses"))

print(conn.execute(stmt).fetchall())

# aliasy
a1 = addresses.alias()
a2 = addresses.alias()
s = select([users]).\
    where(and_(
        users.c.id == a1.c.user_id,
        users.c.id == a2.c.user_id,
        a1.c.email_address == 'jack@msn.com',
        a2.c.email_address == 'jack@yahoo.com'
    ))
conn.execute(s).fetchall()

# joiny
# inner join
s = select([users.c.fullname]).select_from(
    users.join(addresses,
               addresses.c.email_address.like(users.c.name + '%'))
)
print(conn.execute(s).fetchall())

# outer join
s = select([users.c.fullname]).select_from(users.outerjoin(addresses))
print(s)

# fajne bind parametry
s = select([users, addresses]).\
    where(
    or_(
        users.c.name.like(
            bindparam('name', type_=String) + text("'%'")),
        addresses.c.email_address.like(
            bindparam('name', type_=String) + text("'@%'"))
    )
).\
    select_from(users.outerjoin(addresses)).\
    order_by(addresses.c.id)
print(conn.execute(s, name='jack').fetchall())
