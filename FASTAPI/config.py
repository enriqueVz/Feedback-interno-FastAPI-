class Settings:
    CLIENT_ID = "" # -- ID de cliente de la aplicación registrada en Microsoft Entra ID
    CLIENT_SECRET = "" # -- Screto de cliente (valor secreto que se utiliza junto con el ID de cliente para autenticar una aplicación)
    AUTHORITY = "https://login.microsoftonline.com/tu-tenant-id" # --  URL de la autoridad de Microsoft Entra ID
    REDIRECT_URI = "" # --  URL a la que Microsoft Entra ID redirige después de un inicio de sesión 
    SCOPE = ["User.Read"] # -- Permisos o alcances que la aplicación solicitará

settings = Settings()