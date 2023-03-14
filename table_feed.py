from database import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey, orm
from table_user import User
from table_post import Post

class Feed(Base):
    __tablename__ = "feed_action"
    user_id = Column(Integer, ForeignKey(User.id), primary_key = True)
    post_id = Column(Integer, ForeignKey(Post.id), primary_key = True)
    time = Column(TIMESTAMP)
    action = Column(String)
    user = orm.relationship(User)
    post = orm.relationship(Post)