# models/data_record.py
from pydantic import BaseModel, ValidationError, field_validator  # Use field_validator instead of validator

class DataRecord(BaseModel):
    field1: str
    field2: int
    field3: float

    # Pydantic V2 style field validation
    @field_validator('field2')
    def field2_must_be_positive(cls, value):
        if value < 0:
            raise ValueError('field2 must be positive')
        return value
