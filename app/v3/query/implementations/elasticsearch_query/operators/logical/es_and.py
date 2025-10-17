from v3.query.base_query.operators.logical.base_and import BaseAnd
from v3.query.base_query.operators.logical.base_logical import BaseLogical


class And(BaseAnd):
    def create_query(self):
        children_queries = []
        for value in self.values:
            child_query = value.create_query()
            if isinstance(value, BaseLogical):
                child_query = {"bool": child_query}
            children_queries.append(child_query)
        return {"must": children_queries}
