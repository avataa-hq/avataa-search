from v3.query.base_query.operators.comparison.base_in import BaseIn


class In(BaseIn):
    def create_query(self):
        return {"terms": {self.key: self.value}}
