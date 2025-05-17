from sqlalchemy import create_engine

def load(df):
    engine = create_engine('postgresql://username:password@localhost:5432/dbname')
    df.to_sql("sih", engine)
    df.head(5)
    return df