from v3.query.base_query.operators.comparison.base_ne import BaseNe


class Ne(BaseNe):
    def create_query(self):
        return {"bool": {"must_not": [{"term": {self.key: self.value}}]}}
