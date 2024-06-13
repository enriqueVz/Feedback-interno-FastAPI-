from fastapi import FastAPI, Depends, HTTPException, status
from routers import trabajadores, feedbacks
from fastapi.security import OAuth2AuthorizationCodeBearer # -- Para manejar el flujo de autenticación OAuth2 Authorization Code
from jose import JWTError, jwt # -- Para manejar y validar tokens JWT
from msal import ConfidentialClientApplication # -- Para manejar la autenticación con Microsoft Entra ID
from config import settings


app = FastAPI()

app.include_router(trabajadores.router)
app.include_router(feedbacks.router)

# Creamos la clase OAuth2AuthorizationCodeBearer para manejar el flujo de 
# autorización OAuth2
oauth2_scheme = OAuth2AuthorizationCodeBearer( 
    authorizationUrl=f"{settings.AUTHORITY}/oauth2/v2.0/authorize", # <-- URL donde el usuario será dirigido
    tokenUrl=f"{settings.AUTHORITY}/oauth2/v2.0/token" # <-- URL donde la aplicación solicita el token de acceso
)

# -- Creamos una función que obtenga la aplicación MSAL. Autenticación y adquisicicón de tokens - Microsoft Entra ID
def get_msal_app():
    return ConfidentialClientApplication( 
        settings.CLIENT_ID,
        authority=settings.AUTHORITY,
        client_credential=settings.CLIENT_SECRET,
    )


# -- Función para obtener el usuario actual
async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, "", options={"verify_signature": False})
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


# # --Endpoint para Obtener la URL de Autorización
# @app.get("/login")
# def login():
#     msal_app = get_msal_app()
#     auth_url = msal_app.get_authorization_request_url(settings.SCOPE, redirect_uri=settings.REDIRECT_URI)
#     return {"auth_url": auth_url}


# # -- Endpoint para Obtener el Token de Acceso
# @app.get("/token")
# def get_token(code: str):
#     msal_app = get_msal_app()
#     result = msal_app.acquire_token_by_authorization_code(code, scopes=settings.SCOPE, redirect_uri=settings.REDIRECT_URI)
#     if "access_token" in result:
#         return {"access_token": result["access_token"]}
#     else:
#         raise HTTPException(status_code=400, detail="Could not acquire token")

# # -- Endpoint Protegido
# @app.get("/protected")
# async def protected_route(current_user: dict = Depends(get_current_user)):
#     return {"message": "You are authenticated", "user": current_user}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
