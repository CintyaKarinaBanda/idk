# Configuración de AWS
AWS_CONFIG = {
    "REGION": "us-east-1",
    "ROLE_ARN": "arn:aws:iam::123456789012:role/EstafetaMonitoringRole",  # Reemplazar con el ARN del rol real
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
    "COPIAS": []
    # Descomentado: "COPIAS": ['benjamin.calvillo@xaldigital.com', "josue.jimenez@xaldigital.com", "alejandro.romero@xaldigital.com", "alejandro.calixto@xaldigital.com"]
}

# Configuración de reportes
REPORT_CONFIG = {
    "HORAS_CUSTOM": 12,
    "EXCEL_DIR": "excel",
    "DEFAULT_KEYWORD": "EST"
}

# Configuración de estilos para Excel
EXCEL_STYLES = {
    "HEADER_COLOR": "4472C4",
    "HEADER_FONT_COLOR": "FFFFFF",
    "CRITICAL_COLOR": "FF0000",
    "WARNING_COLOR": "FFA500",
    "INFO_COLOR": "0000FF",
    "CHART_STYLE": 10,
    "CHART_HEIGHT": 10,
    "CHART_WIDTH": 10
}