from sqlalchemy import create_engine

def get_engine():
    user = "guslina"
    password = "1234"
    host = "localhost"
    port = "5432"
    db = "fintech"

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    return engine