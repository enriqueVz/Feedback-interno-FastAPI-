from pydantic import BaseModel
from datetime import date
from typing import Optional

# Definición del modelo de datos para un feedback
class Feedback(BaseModel):
    FEEDBACK_ID: Optional[int] = None
    FECHA: date
    PUNT_SKILLS: int
    DESC_SKILLS: Optional[str] = None
    PUNT_TEAMWORK: int
    DESC_TEAMWORK: Optional[str] = None
    PUNT_EMPATHY: int
    DESC_EMPATHY: Optional[str] = None
    PUNT_MOTIVATION: int
    DESC_MOTIVATION: Optional[str] = None
    EMAIL_EVALUADOR: str
    EMAIL_EVALUADO: str