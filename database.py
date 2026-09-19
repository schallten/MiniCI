from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://user:password@localhost:5432/myapp",
    echo=True,
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    print(result.scalar())