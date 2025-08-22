#!/usr/bin/env python3
"""
Script para limpiar duplicados de la base de datos de alertas
"""

from functions.db_manager import eliminar_duplicados, verificar_tabla

def main():
    print("🧹 === LIMPIEZA DE DUPLICADOS ===")
    
    # Verificar estado inicial
    print("📊 Estado inicial:")
    verificar_tabla()
    
    # Eliminar duplicados
    print("\n🗑️ Eliminando duplicados...")
    eliminados = eliminar_duplicados()
    
    # Verificar estado final
    print("\n📊 Estado final:")
    verificar_tabla()
    
    if eliminados > 0:
        print(f"✅ Limpieza completada: {eliminados} duplicados eliminados")
    else:
        print("✅ No se encontraron duplicados")

if __name__ == "__main__":
    main()