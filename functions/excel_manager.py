import os
import pandas as pd
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.chart import PieChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.utils import get_column_letter
from config import REPORT_CONFIG, EXCEL_STYLES

def aplicar_formato(ws, filas, columnas, titulo=None):
    # Bordes y alineación
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                         top=Side(style='thin'), bottom=Side(style='thin'))
    
    for row in ws.iter_rows(min_row=filas[0], max_row=filas[1], min_col=columnas[0], max_col=columnas[1]):
        for cell in row:
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', vertical='center')
    
    # Encabezados - solo la primera fila
    for col in range(columnas[0], columnas[1] + 1):
        cell = ws.cell(row=filas[0], column=col)
        cell.font = Font(bold=True, color=EXCEL_STYLES["HEADER_FONT_COLOR"])
        cell.fill = PatternFill(start_color=EXCEL_STYLES["HEADER_COLOR"], 
                              end_color=EXCEL_STYLES["HEADER_COLOR"], fill_type="solid")
    
    # Ancho columnas
    for col in range(columnas[0], columnas[1] + 1):
        ws.column_dimensions[get_column_letter(col)].width = 18
    
    # Título
    if titulo:
        ws.merge_cells(start_row=filas[0]-2, start_column=columnas[0], 
                      end_row=filas[0]-2, end_column=columnas[1])
        cell = ws.cell(row=filas[0]-2, column=columnas[0])
        cell.value = titulo
        cell.font = Font(bold=True, size=14)
        cell.alignment = Alignment(horizontal='center')

def generar_recomendaciones(df):
    if len(df) == 0:
        return ["No hay datos suficientes para generar recomendaciones."]
    
    recomendaciones = []
    
    # Alertas críticas
    criticas = df[df['Estado'] == 'Critica']
    if len(criticas) > 0:
        # Métricas problemáticas
        top_metricas = criticas.groupby('Metrica').size().sort_values(ascending=False).head(2)
        for metrica, count in top_metricas.items():
            recomendaciones.append(f"⚠️ La métrica '{metrica}' ha generado {count} alertas críticas.")
        
        # Cuentas con problemas
        top_cuentas = criticas.groupby('Nombre cuenta').size().sort_values(ascending=False).head(1)
        for cuenta, count in top_cuentas.items():
            recomendaciones.append(f"⚠️ La cuenta '{cuenta}' tiene {count} alertas críticas.")
    else:
        recomendaciones.append("✅ No se detectaron alertas críticas en el período.")
    
    return recomendaciones

def crear_grafico_circular(ws, row_start, resumen, chart_data):
    # Crear un gráfico circular simple
    chart = PieChart()
    chart.title = "Distribución de Alertas por Severidad"
    chart.style = EXCEL_STYLES["CHART_STYLE"]
    chart.height = EXCEL_STYLES["CHART_HEIGHT"]  # Altura en unidades
    chart.width = EXCEL_STYLES["CHART_WIDTH"]   # Ancho en unidades
    
    # Referencias a los datos y etiquetas
    data_ref = Reference(ws, min_col=3, max_col=3, 
                       min_row=row_start+len(resumen)+6, 
                       max_row=row_start+len(resumen)+5+len(chart_data))
    labels_ref = Reference(ws, min_col=2, 
                         min_row=row_start+len(resumen)+6, 
                         max_row=row_start+len(resumen)+5+len(chart_data))
    
    # Configurar el gráfico
    chart.add_data(data_ref)
    chart.set_categories(labels_ref)
    
    # Mostrar porcentajes en las etiquetas
    chart.dataLabels = DataLabelList()
    chart.dataLabels.showPercent = True
    chart.dataLabels.showVal = False
    chart.dataLabels.showCatName = False
    
    # Calcular posición relativa para la gráfica
    chart_position = row_start + len(resumen) + len(chart_data) + 10
    
    # Añadir el gráfico a la hoja en posición relativa
    ws.add_chart(chart, f"E{chart_position}")

def generar_excel(df, resumen, periodo, horas=None):
    try:
        sufijo = f"_ultimas_{horas}h" if horas else ""
        archivo = f'{REPORT_CONFIG["EXCEL_DIR"]}/Alertas_{periodo}{sufijo}.xlsx'
        
        # Asegurar que el directorio de Excel existe
        os.makedirs(REPORT_CONFIG["EXCEL_DIR"], exist_ok=True)

        with pd.ExcelWriter(archivo, engine='openpyxl') as writer:
            # Detalle
            if not df.empty:
                columnas = ['Id cuenta', 'Nombre cuenta', 'Metrica', 'Estado', 'Fecha_str']
                for col in ['Región', 'Motivo']:
                    if col in df.columns and df[col].notna().any():
                        columnas.append(col)
                
                df_detalle = df[columnas].rename(columns={
                    'Id cuenta': 'ID Cuenta', 
                    'Nombre cuenta': 'Nombre Cuenta', 
                    'Metrica': 'Métrica',
                    'Fecha_str': 'Fecha'
                })
                
                df_detalle.to_excel(writer, sheet_name='Detalle', index=False)
                aplicar_formato(writer.sheets['Detalle'], [1, len(df_detalle) + 1], [1, len(df_detalle.columns)])
            else:
                pd.DataFrame({'Mensaje': ['No hay alertas en el período seleccionado']}).to_excel(
                    writer, sheet_name='Detalle', index=False)
            
            # Resumen con Dashboard
            pd.DataFrame().to_excel(writer, sheet_name='Resumen', index=False)
            ws2 = writer.sheets['Resumen']
            
            # KPIs
            total = len(df)
            criticas = len(df[df['Estado'] == 'Critica'])
            warnings = len(df[df['Estado'] == 'Warning'])
            info = len(df[df['Estado'] == 'Informativo'])
            
            # Porcentajes
            pct_criticas = (criticas / total * 100) if total > 0 else 0
            pct_warnings = (warnings / total * 100) if total > 0 else 0
            pct_info = (info / total * 100) if total > 0 else 0
            
            # Dashboard
            ws2['B2'] = "DASHBOARD DE ALERTAS"
            ws2['B2'].font = Font(bold=True, size=16)
            ws2.merge_cells('B2:F2')
            ws2['B2'].alignment = Alignment(horizontal='center')
            
            # KPIs
            ws2['B4'], ws2['C4'], ws2['D4'], ws2['E4'] = "Total Alertas", "Críticas", "Warnings", "Informativas"
            ws2['B5'], ws2['C5'], ws2['D5'], ws2['E5'] = total, criticas, warnings, info
            ws2['C6'], ws2['D6'], ws2['E6'] = f"{pct_criticas:.1f}%", f"{pct_warnings:.1f}%", f"{pct_info:.1f}%"
            
            # Formato KPIs
            ws2['B5'].font = Font(bold=True, size=14)
            ws2['C5'].font = Font(bold=True, size=14, color=EXCEL_STYLES["CRITICAL_COLOR"])
            ws2['D5'].font = Font(bold=True, size=14, color=EXCEL_STYLES["WARNING_COLOR"])
            ws2['E5'].font = Font(bold=True, size=14, color=EXCEL_STYLES["INFO_COLOR"])
            
            for col in ['B', 'C', 'D', 'E']:
                ws2[f'{col}4'].font = Font(bold=True)
                for row in range(4, 7):
                    ws2.cell(row=row, column=ord(col)-64).alignment = Alignment(horizontal='center')
                    ws2.cell(row=row, column=ord(col)-64).border = Border(
                        left=Side(style='thin'), right=Side(style='thin'),
                        top=Side(style='thin'), bottom=Side(style='thin')
                    )
            
            # Recomendaciones
            ws2.cell(row=8, column=2, value="RECOMENDACIONES:")
            ws2.cell(row=8, column=2).font = Font(bold=True)
            
            for i, rec in enumerate(generar_recomendaciones(df)[:3]):
                ws2.cell(row=9+i, column=2, value=rec)
            
            row_start = 13  # Después de recomendaciones
            
            # Análisis por servicio
            if not df.empty and 'Namespace' in df.columns and df['Namespace'].notna().any():
                try:
                    servicios = df.groupby(['Namespace', 'Estado']).size().unstack(fill_value=0).reset_index()
                    servicios = servicios.rename(columns={'Namespace': 'Servicio AWS'})
                    
                    for estado in ['Critica', 'Warning', 'Informativo']:
                        if estado not in servicios.columns:
                            servicios[estado] = 0
                    
                    servicios['Total'] = servicios[['Critica', 'Warning', 'Informativo']].sum(axis=1)
                    servicios = servicios.sort_values('Total', ascending=False)
                    
                    # Título
                    ws2.cell(row=row_start, column=2, value="ANÁLISIS POR SERVICIO AWS")
                    ws2.cell(row=row_start, column=2).font = Font(bold=True, size=14)
                    ws2.merge_cells(start_row=row_start, start_column=2, end_row=row_start, end_column=6)
                    ws2.cell(row=row_start, column=2).alignment = Alignment(horizontal='center')
                    
                    # Encabezados
                    headers = ['Servicio AWS', 'Critica', 'Warning', 'Informativo', 'Total']
                    for j, header in enumerate(headers):
                        ws2.cell(row=row_start+2, column=2+j, value=header)
                    
                    # Datos
                    for i, row in enumerate(servicios.itertuples(index=False)):
                        for j, val in enumerate(row):
                            ws2.cell(row=row_start+3+i, column=2+j, value=val)
                    
                    aplicar_formato(ws2, [row_start+2, row_start+3+len(servicios)-1], 
                                   [2, 2+len(headers)-1])
                    
                    row_start = row_start + 5 + len(servicios)
                except Exception as e:
                    print(f"Error en análisis de servicios: {e}")
            
            # Resumen por cuenta
            if not resumen.empty:
                # Título
                ws2.cell(row=row_start, column=2, value="RESUMEN POR CUENTA Y MÉTRICA")
                ws2.cell(row=row_start, column=2).font = Font(bold=True, size=14)
                ws2.merge_cells(start_row=row_start, start_column=2, end_row=row_start, end_column=6)
                ws2.cell(row=row_start, column=2).alignment = Alignment(horizontal='center')
                
                # Encabezados
                headers = ['ID Cuenta', 'Nombre Cuenta', 'Métrica', 'Critica', 'Warning', 'Informativo', 'Total']
                for j, header in enumerate(headers):
                    ws2.cell(row=row_start+2, column=2+j, value=header)
                
                # Datos
                for i, row in enumerate(resumen.itertuples(index=False)):
                    for j, val in enumerate(row):
                        ws2.cell(row=row_start+3+i, column=2+j, value=val)
                
                aplicar_formato(ws2, [row_start+2, row_start+3+len(resumen)-1], 
                               [2, 2+len(headers)-1])
                
                # Gráfico circular
                if all(col in resumen.columns for col in ['Critica', 'Warning', 'Informativo']):
                    try:
                        # Datos para gráfico - solo incluir valores > 0
                        chart_data = []
                        if criticas > 0:
                            chart_data.append(("Críticas", criticas))
                        if warnings > 0:
                            chart_data.append(("Warnings", warnings))
                        if info > 0:
                            chart_data.append(("Informativas", info))
                        
                        # Escribir encabezados
                        ws2.cell(row=row_start+len(resumen)+5, column=2, value="Estado")
                        ws2.cell(row=row_start+len(resumen)+5, column=3, value="Cantidad")
                        
                        # Escribir datos
                        for i, (label, value) in enumerate(chart_data):
                            ws2.cell(row=row_start+len(resumen)+6+i, column=2, value=label)
                            ws2.cell(row=row_start+len(resumen)+6+i, column=3, value=value)
                        
                        # Aplicar formato a la tabla
                        aplicar_formato(ws2, 
                                      [row_start+len(resumen)+5, row_start+len(resumen)+5+len(chart_data)], 
                                      [2, 3])
                        
                        # Gráfico
                        if criticas + warnings + info > 0:
                            try:
                                crear_grafico_circular(ws2, row_start, resumen, chart_data)
                            except Exception as e:
                                print(f"Error en gráfico: {e}")
                    except Exception as e:
                        print(f"Error en datos para gráfico: {e}")
        
        return True
    except Exception as e:
        print(f"Error al generar Excel: {e}")
        return False