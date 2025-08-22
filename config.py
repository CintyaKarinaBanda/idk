# Configuración de AWS
AWS_CONFIG = {
    "REGION": "us-east-1",
    "ROLE_ARN": "arn:aws:iam::883278715161:role/ExtractData", 
    "ROLE_SESSION_NAME": "EstafetaMonitoringSession"
}

# Configuración de Gmail
GMAIL_CONFIG = {
    "SCOPES": ['https://www.googleapis.com/auth/gmail.readonly'],
    "TOKEN_PATH": 'credentials/token.json',
    "CREDENTIALS_PATH": 'credentials/credentials.json'
}

# Configuración de correo electrónico
EMAIL_CONFIG = {
    "REMITENTE": "karina.banda@xaldigital.com",
    "PASSWORD": "efci nhmi nzmj ipjk",
    "DESTINATARIO": "karina.banda@xaldigital.com",
    #"DESTINATARIO": "patricia.castillo@xaldigital.com",
    "COPIAS": []
    #"COPIAS": ['infraestructura@xaldigital.com', 'automation-nextgen@xaldigital.com']
}

# Configuración de reportes
REPORT_CONFIG = {
    "HORAS_CUSTOM": 12,
    "EXCEL_DIR": "excel",
    "DEFAULT_KEYWORD": "EST"
}

# Configuración de la base de datos
DB_CONFIG = {
    "HOST": "34.229.52.197",
    #"HOST": "localhost",               
    "DATABASE": "alarmasdb",         
    "USER": "alarmas",                 
    "PASSWORD": "Estafeta_1",          
    "PORT": "5432"                     
}

# Configuración de estilos de Excel
EXCEL_STYLES = {
    "HEADER_COLOR": "4472C4",
    "HEADER_FONT_COLOR": "FFFFFF",
    "CRITICAL_COLOR": "FF0000",  
    "WARNING_COLOR": "FFA500",  
    "INFO_COLOR": "00B050",    
    "CHART_STYLE": 10,
    "CHART_HEIGHT": 10,
    "CHART_WIDTH": 10,
    "CHART_COLORS": ["FF0000", "FFA500", "00B050"]  
}