from v3.query.base_query.operators.comparison.base_eq import BaseEq


class Eq(BaseEq):
    def create_query(self):
        return {"term": {self.key: self.value}}
