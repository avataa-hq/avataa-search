from v3.query.base_query.operators.comparison.base_gte import BaseGte


class Gte(BaseGte):
    def create_query(self):
        return {"range": {self.key: {"gte": self.value}}}
