from services.zeebe_services.kafka.consumers.process_instance_exporter.intents import (
    ZeebeProcessInstanceIntents,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.process_instance_intents.activated import (
    with_process_instance_activated,
)

from services.zeebe_services.kafka.consumers.process_instance_exporter.process_instance_intents.completed import (
    with_process_instance_completed,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.process_instance_intents.externally_terminated import (  # noqa
    with_process_instance_externally_terminated,
)
from services.zeebe_services.kafka.consumers.process_instance_exporter.process_instance_intents.terminated import (
    with_process_instance_terminated,
)

PROCESS_ONLY_BPMN_ELEMENT_TYPE = "PROCESS"

PROCESS_HANDLER_BY_EVENT = {
    ZeebeProcessInstanceIntents.ELEMENT_ACTIVATED.value: with_process_instance_activated,
    ZeebeProcessInstanceIntents.ELEMENT_COMPLETED.value: with_process_instance_completed,
    ZeebeProcessInstanceIntents.ELEMENT_TERMINATED.value: with_process_instance_terminated,
    ZeebeProcessInstanceIntents.EXTERNALLY_TERMINATED.value: with_process_instance_externally_terminated,
}

EVENT_HANDLERS_BY_BPMN_ELEMENT_TYPE = {"PROCESS": PROCESS_HANDLER_BY_EVENT}
