from v3.query.base_query.operators.comparison.base_lte import BaseLte


class Lte(BaseLte):
    def create_query(self):
        return {"range": {self.key: {"lte": self.value}}}
