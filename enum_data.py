from enum import Enum
from pydantic import BaseModel
from typing import List

class QAObject(BaseModel):
    qId: int
    answer: str

class QABody(BaseModel):
    loginid: str
    outletcode: str
    payload: List[QAObject]

class FileNameKey(Enum):
    OPPORTUNITY_OUTLETS = "harshprincegoogleparser/{}_PJP_opportunitiesOutlets.csv"
    QUESTIONS = "harshprincegoogleparser/{}_FAQ.csv"
    ANSWERS = "harshprincegoogleparser/{}_QA.csv"

class ReturnDataType(Enum):
    ARRAY = "array"  
    OBJECT = "object"  
