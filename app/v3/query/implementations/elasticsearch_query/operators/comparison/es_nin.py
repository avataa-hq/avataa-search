from v3.query.base_query.operators.comparison.base_nin import BaseNin


class Nin(BaseNin):
    def create_query(self):
        return {"bool": {"must_not": [{"terms": {self.key: self.value}}]}}
