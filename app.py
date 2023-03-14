from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List
from database import SessionLocal
from table_post import Post
from table_user import User
from table_feed import Feed
from schema import UserGet, PostGet, FeedGet
import os
from catboost import CatBoostClassifier
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime


app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db
    
def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def load_features() -> pd.DataFrame:
    liked_posts = batch_load_sql(
        f"""SELECT u.user_id, f.post_id
        FROM user_data u 
        LEFT JOIN LATERAL (SELECT *,
        ROW_NUMBER() OVER(PARTITION BY user_id) AS order
        FROM feed_data WHERE action = 'like' ) f on u.user_id = f.user_id
        WHERE f.order <= 10 """)
    
    posts_features = batch_load_sql(
    """SELECT * FROM bruhwalkk_post_features_22 """)
    
    user_features = batch_load_sql(
        """SELECT * FROM public.user_data """)
    
    return (user_features, post_features, liked_posts)

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("final_project/catboost_svd2")
    from_file = CatBoostClassifier()
    from_file.load_model(model_path)
    return from_file

features = load_features()

def get_recommended_feed(id: int, time: datetime, limit: int):
   
    user_features = features[0].loc[features[0].user_id == id]
    user_features['hour'] = time.hour
    user_features['month'] = time.month
    user_features['day'] = time.day
    
    user_features = user_features.drop("user_id", axis=1)
    
    cols_to_move = list(user_features.columns)
    
    posts_features = features[1].drop(['index', 'text'], axis=1)
    content = features[1][['post_id', 'text', 'topic']]
    
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')
    
    user_posts_features = user_posts_features[cols_to_move + [x for x in user_posts_features.columns if x not in cols_to_move]]
    
    liked_posts = liked_posts[features[2].user_id == id].post_id.values
    
    model_path = "catboost_svd2"
    from_file = CatBoostClassifier()
    from_file.load_model(model_path)
    
    
    predicts = from_file.predict_proba(user_posts_features)[:,1]
    user_posts_features["pred"] = predicts
    
    recommended_posts = user_posts_features.sort_values('pred')[-limit:].index
    
    return [PostGet(**{
                'id': i,
                'text': content[content.post_id == i].text.values[0],
                'topic': content[content.post_id == i].topic.values[0]
            }) for i in recommended_posts]



@app.get("/user/{id}", response_model=UserGet)
def get_users(id, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(User).filter(User.id == id).limit(limit).one_or_none()
    if not result:
        raise HTTPException(404, "user not found")
    else:
        return result

@app.get("/post/{id}", response_model=PostGet)
def get_post(id, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(Post).filter(Post.id == id).limit(limit).one_or_none()
    if not result:
        raise HTTPException(404, "post not found")
    else:
        return result

@app.get("/user/{id}/feed", response_model=List[FeedGet])
def get_feed_user(id, limit: int = 10,  db: Session = Depends(get_db)):
    result = db.query(Feed).filter(Feed.user_id == id).order_by(Feed.time.desc()).limit(limit).all()
    if result:
        return result
    else:
        raise HTTPException(200, [])

@app.get("/post/{id}/feed", response_model=List[FeedGet])
def get_feed_user(id, limit: int = 10,  db: Session = Depends(get_db)):
    result = db.query(Feed).filter(Feed.post_id == id).order_by(Feed.time.desc()).limit(limit).all()
    if result:
        return result
    else:
        raise HTTPException(200, [])

@app.get("/post/recommendations/", response_model=List[PostGet])
def get_recommendations(id: int, time: datetime = datetime.now(), limit: int = 5) -> Response:
    return get_recommended_feed(id, time, limit)

