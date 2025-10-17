from v3.query.base_query.operators.evaluation.base_regex import BaseRegex


class Regex(BaseRegex):
    def create_query(self):
        return {
            "regexp": {
                self.key: {"value": self.value, "case_insensitive": True}
            }
        }
