from pydantic import BaseModel
from typing import Optional

# Definición del modelo de datos para un trabajador
class Trabajador(BaseModel):
    NOMBRE: str
    APELLIDOS: str
    EMAIL: str
    VERTICAL: str
    COHORTE : str
    PUESTO: str
