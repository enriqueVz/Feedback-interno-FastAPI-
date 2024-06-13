from fastapi import APIRouter, HTTPException, Depends
from models.feedback import Feedback
from db.connection import get_snowflake_connection
import snowflake.connector

router = APIRouter()

#Obtener todos los feedbacks. 
@router.get("/feedback/")
def read_feedbacks(db: snowflake.connector.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Creación del cursor para situarse en la base de datos
        cursor = db.cursor()
        cursor.execute("SELECT * FROM FACTS_FEEDBACK ORDER BY FECHA DESC")
        #Se recuperan las filas del resultado de la query
        trabajadores = cursor.fetchall()
        #Devuelve todos los datos de todos los feedbacks como json
        return trabajadores
    except Exception as e:
        print(f"Error al leer los datos desde Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Error al leer los datos")

#Crear nuevo feedback. Recibe por parámetro los datos del feedback y mediante la ejecución de la query SQL se registra en FACTS_FEEDBACK y en DIM_FECHA
@router.post("/feedback/")
def insert_new_feedback(feedback: Feedback, db: snowflake.connector.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        cursor = db.cursor()
        query = """INSERT INTO FACTS_FEEDBACK (FECHA, PUNT_SKILLS, DESC_SKILLS, PUNT_TEAMWORK, DESC_TEAMWORK, 
                PUNT_EMPATHY, DESC_EMPATHY, PUNT_MOTIVATION, DESC_MOTIVATION, EMAIL_EVALUADOR, EMAIL_EVALUADO)
                VALUES (CURRENT_DATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        #Ejecutar la query pasando como parámetros en VALUES() el valor de cada campo del feedback que entra como parámetro        
        cursor.execute(query, (
            feedback.PUNT_SKILLS, feedback.DESC_SKILLS, feedback.PUNT_TEAMWORK,
            feedback.DESC_TEAMWORK, feedback.PUNT_EMPATHY, feedback.DESC_EMPATHY,
            feedback.PUNT_MOTIVATION, feedback.DESC_MOTIVATION,  feedback.EMAIL_EVALUADOR, feedback.EMAIL_EVALUADO
        ))
        #Ejecutar la query con el valor de la fecha de creación del feedback
        query2 = """
        INSERT INTO DIM_FECHA (FECHA) VALUES (CURRENT_DATE())
        """
        cursor.execute(query2,)
        #Se ejecutan ambas queries
        db.commit()
        #Devuelve un mensaje de éxito
        return {"message": "Feedback insertado con éxito"}
    except Exception as e:
        print(f"Error al insertar el nuevo feedback en Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Error al crear el feedback")
    finally:
        cursor.close() 
        
# Obtener  feedback por EMAIL_EVALUADOR. Recibe como parámetro el email de quien hace la evaluación
@router.get("/feedback/evaluador/{EMAIL_EVALUADOR}")
def get_feedback_by_evaluator_email(EMAIL_EVALUADOR: str, db: snowflake.connector.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail_evaluador = EMAIL_EVALUADOR.lower().strip()
        
        cursor = db.cursor()
        #Se compara el valor en minúsculas del email_evaluador de la base de datos con el valor transformado que ha entrado como parámetro dentro de la consulta
        cursor.execute("SELECT * FROM FACTS_FEEDBACK WHERE LOWER(EMAIL_EVALUADOR) = LOWER(%s) ORDER BY FECHA DESC", (mail_evaluador,))
        
        #recupera la fila del resultado de la consulta y la devuelve como tupla
        feedback = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not feedback:
            raise HTTPException(status_code=404, detail="Feedback no encontrado")
        return feedback
    except Exception as e:
        print(f"Error al leer desde Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Error al leer los datos")
    finally:
        cursor.close()
        db.close()

# Obtener feedback por EMAIL_EVALUADO. Recibe como parámetro el email del trabajador evaluado
@router.get("/feedback/evaluado/{EMAIL_EVALUADO}")
def get_feedback_by_evaluated_email(EMAIL_EVALUADO: str, db: snowflake.connector.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforma el valor que entra como parámetro a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail_evaluado = EMAIL_EVALUADO.lower().strip()
        
        cursor = db.cursor()
        #Se compara el valor en minúsculas del email_evaluado de la base de datos con el valor transformado que ha entrado como parámetro dentro de la consulta
        cursor.execute("SELECT * FROM FACTS_FEEDBACK WHERE LOWER(EMAIL_EVALUADO) = LOWER(%s) ORDER BY FECHA DESC", (mail_evaluado,))
        #recupera la fila del de resultado de la consulta y la devuelve como tupla
        feedback = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not feedback:
            raise HTTPException(status_code=404, detail="Feedback no encontrado")
        return feedback
    except Exception as e:
        print(f"Error al leer desde Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Error al leer los datos")
    finally:
        cursor.close()
        db.close()
        
#Obtener feedbacks de evualuador a evaluado. Se recibe como parámetros el email de quien hace el feedback y el email de quien recibe el feedback
@router.get("/feedback/evaluado/{EMAIL_EVALUADOR}/{EMAIL_EVALUADO}")
def get_feedback_by_evaluator_and_evaluated_email(EMAIL_EVALUADOR: str,EMAIL_EVALUADO: str ,db: snowflake.connector.SnowflakeConnection = Depends(get_snowflake_connection)):
    try:
        #Se transforman los valores que entran como parámetros a minúsuclas y eliminando los espacios anteriores y posteriores al valor
        #para su posterior comparación con la base de datos
        mail_evaluador = EMAIL_EVALUADOR.lower().strip()
        mail_evaluado = EMAIL_EVALUADO.lower().strip()
        #recupera la fila del resultado de la consulta y la devuelve como tupla
        cursor = db.cursor()
        #Se comparan los valores en minúsculas de los emails de la base de datos con el valor transformado de ambos emails que han entrado como parámetros dentro de la consulta
        cursor.execute("SELECT * FROM FACTS_FEEDBACK WHERE LOWER(EMAIL_EVALUADOR) = LOWER(%s) AND LOWER(EMAIL_EVALUADO) = LOWER(%s) ORDER BY FECHA DESC", (mail_evaluador, mail_evaluado))
        #recupera la fila del de resultado de la consulta y la devuelve como tupla
        feedback = cursor.fetchone()
        #Si no devuelve resultado de la consulta, se muestra error
        if not feedback:
            raise HTTPException(status_code=404, detail="Feedback no encontrado")
        return feedback
    except Exception as e:
        print(f"Error al leer desde Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Error al leer los datos")
    finally:
        cursor.close()
        db.close()
        