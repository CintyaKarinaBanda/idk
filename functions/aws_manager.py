import boto3
from config import AWS_CONFIG

def setup_aws():
    # Configuramos una sesión con credenciales por defecto
    boto3.setup_default_session(region_name=AWS_CONFIG["REGION"])
    
    # Primero obtenemos información sobre la identidad actual
    try:
        sts_client = boto3.client('sts')
        current_identity = sts_client.get_caller_identity()
        current_account = current_identity.get('Account')
        current_arn = current_identity.get('Arn')
        print(f"ℹ️ Identidad actual: {current_arn} (Cuenta: {current_account})")
        
        # Intentamos asumir el rol especificado
        try:
            print(f"ℹ️ Intentando asumir rol: {AWS_CONFIG['ROLE_ARN']}")
            assumed_role = sts_client.assume_role(
                RoleArn=AWS_CONFIG["ROLE_ARN"],
                RoleSessionName=AWS_CONFIG["ROLE_SESSION_NAME"]
            )
            
            # Configuramos la sesión con las credenciales del rol asumido
            credentials = assumed_role['Credentials']
            boto3.setup_default_session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=AWS_CONFIG["REGION"]
            )
            print(f"✅ Rol asumido correctamente: {AWS_CONFIG['ROLE_ARN']}")
            
            # Obtener nombres de cuentas desde Organizations
            try:
                client = boto3.client('organizations', region_name=AWS_CONFIG["REGION"])  
                accounts = []
                for page in client.get_paginator('list_accounts').paginate():
                    accounts.extend(page['Accounts'])
                return {a['Id']: a['Name'] for a in accounts if a['Status'] == 'ACTIVE'}
            except Exception as e:
                print(f"⚠️ No se pudieron obtener cuentas de Organizations: {e}")
                # Fallback a cuenta actual
                return {current_account: f"Cuenta {current_account}"}
        except Exception as e:
            print(f"❌ Error al asumir el rol: {e}")
            print("ℹ️ Usando identidad actual como fallback")
            return {current_account: f"Cuenta {current_account}"}
    except Exception as e:
        print(f"❌ Error al obtener la identidad actual: {e}")
        return {}