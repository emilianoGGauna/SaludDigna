from sqlalchemy import inspect

inspector = inspect(engine)
print("Tables in database:", inspector.get_table_names())
