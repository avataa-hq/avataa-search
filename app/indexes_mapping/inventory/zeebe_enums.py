from enum import Enum


class ZeebeProcessInstanceFields(Enum):
    PROCESS_DEFINITION_KEY = "processDefinitionKey"
    PROCESS_DEFINITION_VERSION = "processDefinitionVersion"
    PROCESS_DEFINITION_ID = "processDefinitionId"
    PROCESS_INSTANCE_ID = "processInstanceId"
    BUSINESS_KEY = "businessKey"
    START_DATE = "startDate"
    END_DATE = "endDate"
    DURATION = "duration"
    STATE = "state"
