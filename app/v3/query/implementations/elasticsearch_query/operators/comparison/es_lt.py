from v3.query.base_query.operators.comparison.base_lt import BaseLt


class Lt(BaseLt):
    def create_query(self):
        return {"range": {self.key: {"lt": self.value}}}
