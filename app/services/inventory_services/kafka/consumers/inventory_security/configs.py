from services.inventory_services.kafka.consumers.inventory_security.events.mo_permissions import (
    with_mo_permissions_create,
    with_mo_permissions_update,
    with_mo_permissions_delete,
)
from services.inventory_services.kafka.consumers.inventory_security.events.prm_permissions import (
    with_prm_permissions_create,
    with_prm_permissions_update,
    with_prm_permissions_delete,
)
from services.inventory_services.kafka.consumers.inventory_security.events.tmo_permissions import (
    with_tmo_permissions_create,
    with_tmo_permissions_update,
    with_tmo_permissions_delete,
)
from services.inventory_services.kafka.consumers.inventory_security.events.tprm_permissions import (
    with_tprm_permissions_create,
    with_tprm_permissions_update,
    with_tprm_permissions_delete,
)
from services.inventory_services.kafka.consumers.inventory_security.models import (
    InventorySecurityPermissionsScope,
    InventorySecurityEventStatus,
)

MO_PERMISSION_HANDLERS_BY_MSG_EVENT = {
    InventorySecurityEventStatus.CREATED.value: with_mo_permissions_create,
    InventorySecurityEventStatus.UPDATED.value: with_mo_permissions_update,
    InventorySecurityEventStatus.DELETED.value: with_mo_permissions_delete,
}

PRM_PERMISSION_HANDLERS_BY_MSG_EVENT = {
    InventorySecurityEventStatus.CREATED.value: with_prm_permissions_create,
    InventorySecurityEventStatus.UPDATED.value: with_prm_permissions_update,
    InventorySecurityEventStatus.DELETED.value: with_prm_permissions_delete,
}

TMO_PERMISSION_HANDLERS_BY_MSG_EVENT = {
    InventorySecurityEventStatus.CREATED.value: with_tmo_permissions_create,
    InventorySecurityEventStatus.UPDATED.value: with_tmo_permissions_update,
    InventorySecurityEventStatus.DELETED.value: with_tmo_permissions_delete,
}

TPRM_PERMISSION_HANDLERS_BY_MSG_EVENT = {
    InventorySecurityEventStatus.CREATED.value: with_tprm_permissions_create,
    InventorySecurityEventStatus.UPDATED.value: with_tprm_permissions_update,
    InventorySecurityEventStatus.DELETED.value: with_tprm_permissions_delete,
}

INVENTORY_SECURITY_HANDLER_BY_MSG_PERMISSION_SCOPE = {
    InventorySecurityPermissionsScope.MO.value: MO_PERMISSION_HANDLERS_BY_MSG_EVENT,
    InventorySecurityPermissionsScope.TMO.value: TMO_PERMISSION_HANDLERS_BY_MSG_EVENT,
    InventorySecurityPermissionsScope.TPRM.value: TPRM_PERMISSION_HANDLERS_BY_MSG_EVENT,
    InventorySecurityPermissionsScope.PRM.value: PRM_PERMISSION_HANDLERS_BY_MSG_EVENT,
}
