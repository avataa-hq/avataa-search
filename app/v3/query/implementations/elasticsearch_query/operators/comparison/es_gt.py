from v3.query.base_query.operators.comparison.base_gt import BaseGt


class Gt(BaseGt):
    def create_query(self):
        return {"range": {self.key: {"gt": self.value}}}
