from pydantic import BaseModel


class SecurityItem(BaseModel):
    id: int
    parent_id: int | None = None
    permission: str
    permission_name: str
    root_permission_id: int | None = None
    read: bool = False
    delete: bool = False
    active: bool = False
    create: bool = False
    update: bool = False
    admin: bool = False
