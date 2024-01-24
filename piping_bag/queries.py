from typing import Optional
from .interfaces import Database


class BaseQueries:
    def __init__(self, db: Database, schema: Optional[str] = None):
        self.db = db
        self.schema = schema
