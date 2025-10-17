from v3.query.base_query.operators.element.base_exists import BaseExists


class Exists(BaseExists):
    def create_query(self):
        if self.value:
            return {"exists": {"field": self.key}}
        else:
            return {"bool": {"must_not": {"exists": {"field": self.key}}}}
