import json

from fastapi import HTTPException


def get_locale(lang: str):
    try:
        with open(f"./locales/{lang}.json", "rb") as file:
            return json.loads(file.read().decode("utf-8"))
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="Unsupported locale!")
