from v3.query.base_query.operators.logical.base_nor import BaseNor
from v3.query.base_query.operators.logical.base_logical import BaseLogical


class Nor(BaseNor):
    def create_query(self):
        children_queries = []
        for value in self.values:
            child_query = value.create_query()
            if isinstance(value, BaseLogical):
                child_query = {"bool": child_query}
            children_queries.append(child_query)
        return {"must_not": children_queries}
