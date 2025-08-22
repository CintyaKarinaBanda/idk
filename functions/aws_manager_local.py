import boto3
from config import AWS_CONFIG

def setup_aws():
    """
    Configuración simplificada de AWS usando solo credenciales CLI locales
    Sin intentar asumir roles adicionales
    """
    try:
        # Usar credenciales por defecto del CLI
        boto3.setup_default_session(region_name=AWS_CONFIG["REGION"])
        
        # Obtener información de la identidad actual
        sts_client = boto3.client('sts')
        current_identity = sts_client.get_caller_identity()
        current_account = current_identity.get('Account')
        current_arn = current_identity.get('Arn')
        
        print(f"✅ Usando credenciales CLI: {current_arn} (Cuenta: {current_account})")
        
        # Intentar obtener nombres de cuentas desde Organizations (opcional)
        try:
            org_client = boto3.client('organizations', region_name=AWS_CONFIG["REGION"])
            accounts = []
            for page in org_client.get_paginator('list_accounts').paginate():
                accounts.extend(page['Accounts'])
            account_names = {a['Id']: a['Name'] for a in accounts if a['Status'] == 'ACTIVE'}
            print(f"✅ Obtenidos nombres de {len(account_names)} cuentas desde Organizations")
            return account_names
        except Exception as e:
            print(f"⚠️ No se pudieron obtener cuentas de Organizations: {e}")
            # Fallback: usar solo la cuenta actual
            return {current_account: f"Cuenta-{current_account}"}
            
    except Exception as e:
        print(f"❌ Error al configurar AWS: {e}")
        print("ℹ️ Continuando sin nombres de cuentas AWS")
        return {}