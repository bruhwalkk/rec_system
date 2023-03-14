from database import Base, engine, SessionLocal
from sqlalchemy import Column, Integer, String, desc
from sqlalchemy import func

class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key = True)
    gender = Column(Integer)
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    session = SessionLocal()
    l = []

    for user in (
        session.query(User.country, User.os, func.count(User.id))
        .filter(User.exp_group == 3)
        .group_by(User.country, User.os)
        .having(func.count(User.id) > 100)
        .order_by(desc(func.count(User.id)))
        .all()
    ):
        l.append(user)
    print(l)
