from enum import Enum

class JobStatus(Enum):
  CREATED = "CREATED"
  RUNNING = "RUNNING"
  COMPLETED = "COMPLETED"
  FAILED = "FAILED"