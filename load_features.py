import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from sklearn.compose import ColumnTransformer
from category_encoders import TargetEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import TruncatedSVD


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

def transform_features(data, posts):
    processed_df = pd.merge(data, posts, on=('post_id'), how='left')
    return processed_df

def load_features() -> pd.DataFrame:
    data = batch_load_sql(
    """SELECT u.*, f.timestamp, f.post_id 
    FROM user_data u 
    LEFT JOIN LATERAL (SELECT *,
    ROW_NUMBER() OVER(PARTITION BY user_id) AS order
    FROM feed_data WHERE action != 'like' ORDER BY timestamp) f on u.user_id = f.user_id
    WHERE f.order <= 10 """)
    
    posts = batch_load_sql(
    """SELECT * FROM bruhwalkk_post_features_22 """)
    
    return transform_features(data, posts)
    
    