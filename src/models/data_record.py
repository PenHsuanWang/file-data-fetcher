# src/models/data_record.py

from pydantic import BaseModel

class DataRecordCSV(BaseModel):
    name: str
    age: int
    city: str

class DataRecordExcel(BaseModel):
    product: str
    price: float
    quantity: int