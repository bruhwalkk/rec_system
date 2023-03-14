from database import Base, engine, SessionLocal
from sqlalchemy import Column, Integer, String

class Post(Base):
    __tablename__ = "post"
    id = Column(Integer, primary_key = True)
    text = Column(String)
    topic = Column(String)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    session = SessionLocal()

    l = []
    for Post in (session.query(Post).filter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all()):
        l.append(Post.id)
    print(l)