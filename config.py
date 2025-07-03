# Configuración de AWS
AWS_CONFIG = {
    "REGION": "us-east-1",
    "ROLE_ARN": "arn:aws:iam::883278715161:role/ExtractData", 
    "ROLE_SESSION_NAME": "EstafetaMonitoringSession"
}

GMAIL_CONFIG = {
    "SCOPES": ['https://www.googleapis.com/auth/gmail.readonly'],
    "TOKEN_PATH": 'credentials/token.json',
    "CREDENTIALS_PATH": 'credentials/credentials.json'
}

EMAIL_CONFIG = {
    "REMITENTE": "karina.banda@xaldigital.com",
    "PASSWORD": "efci nhmi nzmj ipjk",
    "DESTINATARIO": "karina.banda@xaldigital.com",
    #"DESTINATARIO": "patricia.castillo@xaldigital.com",
    "COPIAS": []
    #"COPIAS": ['infraestructura@xaldigital.com', 'automation-nextgen@xaldigital.com']
}

REPORT_CONFIG = {
    "HORAS_CUSTOM": 12,
    "EXCEL_DIR": "excel",
    "DEFAULT_KEYWORD": "EST"
}

DB_CONFIG = {
    "HOST": "localhost",               
    "DATABASE": "alarmasdb",         
    "USER": "alarmas",                 
    "PASSWORD": "Estafeta_1",          
    "PORT": "5432"                     
}


EXCEL_STYLES = {
    "HEADER_COLOR": "4472C4",
    "HEADER_FONT_COLOR": "FFFFFF",
    "CRITICAL_COLOR": "FF0000",  # Rojo
    "WARNING_COLOR": "FFA500",   # Amarillo/Naranja
    "INFO_COLOR": "00B050",      # Verde
    "CHART_STYLE": 10,
    "CHART_HEIGHT": 10,
    "CHART_WIDTH": 10,
    "CHART_COLORS": ["FF0000", "FFA500", "00B050"]  # Colores para el gráfico: Rojo, Amarillo, Verde
}