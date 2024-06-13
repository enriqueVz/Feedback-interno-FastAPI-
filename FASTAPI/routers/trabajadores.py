from fastapi import APIRouter, HTTPException, Depends
from models.trabajador import Trabajador
from db.connection import get_snowflake_connection
import snowflake.connector
from typing import Optional

router = APIRouter()

#Obtener todos los trabajadores
@router.get("/trabajadores/")
def get_all_workers(db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Creación del cursor para situarse en la base de datos
        cursor = db.cursor()
        cursor.execute("SELECT * FROM LANDING_TRABAJADOR")
        #Se recuperan las filas del resultado de la query
        trabajadores = cursor.fetchall()
        #Devuelve todos los datos de todos los trabajadores como json
        return trabajadores
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        cursor.close()

#Obtener todos los nombres de los trabajadores
@router.get("/trabajadores/name")
def get_all_names(db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Creación del cursor para situarse en la base de datos
        cursor = db.cursor()
        cursor.execute("SELECT NOMBRE, APELLIDOS FROM LANDING_TRABAJADOR")
        #Se recuperan las filas del resultado de la query
        trabajadores = cursor.fetchall()
        #Devuelve todos los nombres de todos los trabajadores como json
        return trabajadores
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        cursor.close()

#Obtener todos los emails de los trabajadores        
@router.get("/trabajadores/email")
def get_all_emails(db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Creación del cursor para situarse en la base de datos
        cursor = db.cursor()
        cursor.execute("SELECT EMAIL FROM LANDING_TRABAJADOR")
        #Se recuperan las filas del resultado de la query
        trabajadores = cursor.fetchall()
        #Devuelve todos los emails de todos los trabajadores como json
        return trabajadores
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        cursor.close()

#Obtener el trabajador por el email. Recibe por parámetro el id, en este caso es el email del trabajador.
@router.get("/trabajadores/{EMAIL}")
def get_worker_by_email(EMAIL : str, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail = EMAIL.lower().strip()
        cursor = db.cursor()
        #Se compara el valor en minúsculas del email de la base de datos con el valor transformado que ha entrado como parámetro dentro de la consulta
        cursor.execute("SELECT * FROM LANDING_TRABAJADOR WHERE LOWER(EMAIL) = LOWER(%s)", (mail,))
        #recupera la fila del resultado de la consulta y la devuelve como tupla
        trabajador = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not trabajador:
            raise HTTPException(status_code=404, detail="Trabajador no encontrado")
        #Devuelve los datos del trabajador
        return trabajador
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        cursor.close()

#Obtener el nombre del trabajador por el email. Recibe como parámetro 
@router.get("/trabajadores/name/{EMAIL}")
def get_worker_name_by_email(EMAIL: str, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail = EMAIL.lower().strip()
        cursor = db.cursor()
        #Se compara el valor en minúsculas del email de la base de datos con el valor transformado que ha entrado como parámetro dentro de la consulta
        cursor.execute("SELECT NOMBRE, APELLIDOS FROM LANDING_TRABAJADOR WHERE LOWER(EMAIL) = LOWER(%s)", (mail,))
        #recupera la fila del resultado de la consulta y la devuelve como tupla
        trabajador = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not trabajador:
            raise HTTPException(status_code=404, detail="Trabajador no encontrado")
        #devuelve el nombre completo del trabajador
        return trabajador
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        cursor.close()

#Obtener JSON del email por el nombre
@router.get("/trabajadores/json/email/{TRABAJADOR_NOMBRE_COMPLETO}")
def get_email_by_name(
    TRABAJADOR_NOMBRE_COMPLETO: str,
    db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)
):
    cursor = None
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        nombre = TRABAJADOR_NOMBRE_COMPLETO.lower().strip()
        cursor = db.cursor()
        #Se compara el valor en minúsculas del nombre completo como único campo, concatenando las 2 columnas
        # de la base de datos, con el valor transformado que ha entrado como parámetro dentro de la consulta
        query = """
            SELECT EMAIL 
            FROM LANDING_TRABAJADOR 
            WHERE LOWER(CONCAT(REPLACE(NOMBRE, ' ', ''), REPLACE(APELLIDOS, ' ', ''))) LIKE LOWER(%s);
        """
        cursor.execute(query, (nombre,))
        #recupera la fila del resultado de la consulta y la devuelve como tupla
        trabajador = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not trabajador:
            raise HTTPException(status_code=404, detail="Trabajador no encontrado")
        #devuelve el email del trabajador
        return {"email": trabajador[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error closing cursor: {e}")
        try:
            db.close()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error closing database connection: {e}")     


# #Obtener email por nombre completo
# @router.get("/trabajadores/{TRABAJADOR_NOMBRE}/{TRABAJADOR_APELLIDO1}")
# def get_worker_email_by_name(
#     TRABAJADOR_NOMBRE: str,
#     TRABAJADOR_APELLIDO1: str,
#     TRABAJADOR_APELLIDO2: Optional[str] = None,
#     db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)
# ):
#     cursor = None
#     try:
#         nombre = TRABAJADOR_NOMBRE.lower().strip()
#         apellido1 = TRABAJADOR_APELLIDO1.lower().strip()

#         cursor = db.cursor()

#         if TRABAJADOR_APELLIDO2:
#             apellido2 = TRABAJADOR_APELLIDO2.lower().strip()
#             query = """
#                 SELECT EMAIL FROM LANDING_TRABAJADOR 
#                 WHERE LOWER(NOMBRE) LIKE %s 
#                 AND LOWER(APELLIDO1) LIKE %s 
#                 AND LOWER(APELLIDO2) LIKE %s
#             """
#             cursor.execute(query, (nombre, apellido1, apellido2))
#         else:
#             query = """
#                 SELECT EMAIL FROM LANDING_TRABAJADOR 
#                 WHERE LOWER(NOMBRE) LIKE %s 
#                 AND LOWER(APELLIDO1) LIKE %s
#             """
#             cursor.execute(query, (nombre, apellido1))

#         trabajador = cursor.fetchall()
#         if not trabajador:
#             raise HTTPException(status_code=404, detail="Trabajador no encontrado")
#         if len(trabajador) > 1:
#             raise HTTPException(status_code=400, detail="Hay más de un trabajador con ese nombre y primer apellido, por favor introduzca el segundo apellido")
#         return {"email": trabajador[0]}
#     except Exception as e:

#         raise HTTPException(status_code=500, detail=f"Error al leer datos desde Snowflake: {e}")
#     finally:
#         if cursor:
#             try:
#                 cursor.close()
#             except Exception as e:

#                 raise HTTPException(status_code=500, detail=f"Error closing cursor: {e}")
#         try:
#             db.close()
#         except Exception as e:

#             raise HTTPException(status_code=500, detail=f"Error closing database connection: {e}")      

#Crear nuevo trabajador. Recibe como parámetro los datos del trabajador y mediante la query se registra en la tabla LANDING_TRABAJADOR
@router.post("/trabajadores/")
def create_worker(trabajador: Trabajador, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        cursor = db.cursor()
        
        # Comprobar si el EMAIL ya existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM LANDING_TRABAJADOR 
            WHERE EMAIL = %s
        """, (trabajador.EMAIL,))
        #recupera la fila del resultado de la consulta 
        result = cursor.fetchone()
        #Si ya existe un trabajador en la base de datos con el email indicado, se muestra el error
        if result[0] > 0:
            raise HTTPException(status_code=400, detail="Ya existe un trabajador con ese email")
        #Ejecutar la query pasando como parámetros en VALUES() el valor de cada campo del trabajador que entra como parámetro
        cursor.execute("""
            INSERT INTO LANDING_TRABAJADOR (NOMBRE, APELLIDOS, EMAIL, VERTICAL, COHORTE, PUESTO)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (trabajador.NOMBRE, trabajador.APELLIDOS, trabajador.EMAIL, trabajador.VERTICAL, trabajador.COHORTE ,trabajador.PUESTO))
        #Se ejecuta la query
        db.commit()
        #devuelve un mensaje de éxito
        return {"message": "Trabajador creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el trabajador en Snowflake: {e}")
    finally:
        cursor.close()

#Actualizar trabajador por id. Recibe por parámetro el email (id) del trabajador para posteriormente recibir los datos de cada campo a actualizar
@router.put("/trabajadores/{EMAIL}")
def update_worker_by_email(EMAIL: str, trabajador: Trabajador, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail = EMAIL.lower().strip()
        cursor = db.cursor()
        #Ejecutar la query pasando como parámetros a actualizar (SET) el valor de cada campo del trabajador que entra como parámetro y el valor del email para el where
        cursor.execute("""
            UPDATE LANDING_TRABAJADOR SET NOMBRE = %s, APELLIDOS = %s,  EMAIL = %s, VERTICAL = %s, COHORTE = %s ,PUESTO = %s
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (trabajador.NOMBRE, trabajador.APELLIDOS, trabajador.EMAIL, trabajador.VERTICAL, trabajador.COHORTE, trabajador.PUESTO, mail))
        #Se ejecuta la query
        db.commit()
        #Devuelve un mensaje de éxito tras actualizar los datos del trabajador en la base de datos
        return {"message": "Trabajador actualizado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar el Trabajador en Snowflake: {e}")
    finally:
        cursor.close()

# #Actualizar trabajador por nombre completo
# @router.put("/trabajadores/{TRABAJADOR_NOMBRE}/{TRABAJADOR_APELLIDO1}}")
# def update_worker_by_name(TRABAJADOR_NOMBRE: str, TRABAJADOR_APELLIDO1: str,  trabajador: Trabajador, TRABAJADOR_APELLIDO2: Optional[str] = None, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
#     try:
#         nombre = TRABAJADOR_NOMBRE.lower().strip()
#         apellido1 = TRABAJADOR_APELLIDO1.lower().strip()
#         cursor = db.cursor()
        
#         if TRABAJADOR_APELLIDO2:
#             apellido2 = TRABAJADOR_APELLIDO2.lower().strip()
#             query = ("""
#             UPDATE LANDING_TRABAJADOR SET NOMBRE = %s, APELLIDO1 = %s, APELLIDO2 = %s, EMAIL = %s, VERTICAL = %s, COHORTE = %s ,PUESTO = %s
#             WHERE LOWER(NOMBRE) = LOWER(%s) AND LOWER(APELLIDO1) = LOWER(%s) AND LOWER(APELLIDO2) = LOWER(%s)
#             """)
#             cursor.execute(query, (trabajador.NOMBRE, trabajador.APELLIDO1, trabajador.APELLIDO2, trabajador.EMAIL,
#                                    trabajador.VERTICAL, trabajador.COHORTE, trabajador.PUESTO, nombre, apellido1, apellido2))
#         else:
#             query = ("""
#             UPDATE LANDING_TRABAJADOR SET NOMBRE = %s, APELLIDO1 = %s, APELLIDO2 = %s, EMAIL = %s, VERTICAL = %s, COHORTE = %s ,PUESTO = %s
#             WHERE LOWER(NOMBRE) = LOWER(%s) AND LOWER(APELLIDO1) = LOWER(%s)  
#             """)
#             cursor.execute(query, (trabajador.NOMBRE, trabajador.APELLIDO1, trabajador.APELLIDO2, trabajador.EMAIL,
#                                    trabajador.VERTICAL, trabajador.COHORTE, trabajador.PUESTO, nombre, apellido1))
#         db.commit()
#         return {"message": "Trabajador actualizado correctamente"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error al actualizar el Trabajador en Snowflake: {e}")
#     finally:
#         cursor.close()

#Borrar trabajador por id. Recibe como parámetro el email (id) del trabajador.
@router.delete("/trabajadores/{EMAIL}")
def delete_worker_by_email(EMAIL: str, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail = EMAIL.lower().strip()
        cursor = db.cursor()
        #Se compara el valor en minúsculas del email de la base de datos con el valor transformado que ha entrado como parámetro dentro de la consulta
        cursor.execute("DELETE FROM LANDING_TRABAJADOR WHERE LOWER(EMAIL) = LOWER(%s)", (mail,))
        #Se ejecuta la query
        db.commit()
        #Devuelve mensaje de éxito tras eliminar al trabajador de la base de datos.
        return {"message": "Trabajador eliminado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar el trabajador en Snowflake: {e}")
    finally:
        cursor.close()

# #Borrar trabajador por nombre completo
# @router.delete("/trabajadores/{TRABAJADOR_NOMBRE}/{TRABAJADOR_APELLIDO1}/{TRABAJADOR_APELLIDO2}")
# def delete_worker_by_name(TRABAJADOR_NOMBRE: str, TRABAJADOR_APELLIDO1: str, TRABAJADOR_APELLIDO2: str, db: snowflake.connector.connection.SnowflakeConnection = Depends(get_snowflake_connection)):
#     try:
#         nombre = TRABAJADOR_NOMBRE.lower().strip()
#         apellido1 = TRABAJADOR_APELLIDO1.lower().strip()
#         apellido2 = TRABAJADOR_APELLIDO2.lower().strip()
#         cursor = db.cursor()
#         cursor.execute("""
#             DELETE FROM LANDING_TRABAJADOR 
#             WHERE LOWER(NOMBRE) = LOWER(%s) AND LOWER(APELLIDO1) = LOWER(%s) AND LOWER(APELLIDO2) = LOWER(%s)
#         """, (nombre, apellido1, apellido2))
#         db.commit()
#         return {"message": "Trabajador eliminado correctamente"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error al eliminar el trabajador en Snowflake: {e}")
#     finally:
#         cursor.close()
