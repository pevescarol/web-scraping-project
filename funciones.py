from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd
import os
import platform



def cargar_datos_proyecciones():
    
    html_text = requests.get('https://www.bcr.com.ar/es/mercados/gea/estimaciones-nacionales-de-produccion/estimaciones').text
    #print(html_text) #response 200
    soup = BeautifulSoup(html_text, 'lxml')
    estimaciones = soup.find('div', class_ = 'table-estimaciones-responsive')

    #me trae todas las tablas de cada cultivo
    #cultivos = estimaciones.find_all('table', class_ = 'bcr-estimaciones')
##################################3
    #Tabla del Trigo
    trigo = estimaciones.find('table', class_ = 'bcr-estimaciones trigo color')

    listado_header = []
    cabecera_trigo = trigo.thead.find_all('th')
    for head in cabecera_trigo:
        listado_header.append(head.text)

    listado_ultimo_anio = []
    ultimo_anio = trigo.tbody.find_all('tr')[0]
    data1 = ultimo_anio.find_all('td')
    for d in data1:
        listado_ultimo_anio.append(d.text)

    listado_anio_anterior = []
    ultimo_anio = trigo.tbody.find_all('tr')[1]
    data2 = ultimo_anio.find_all('td')
    for d2 in data2:
        listado_anio_anterior.append(d2.text)

    trigo_ultimo_anio = (listado_header[0], listado_ultimo_anio[0], listado_ultimo_anio[1].replace('MILLONES HA', ' MILLONES HA'), listado_ultimo_anio[2].replace('QQ/HA', ' QQ/HA'), listado_ultimo_anio[3].replace('MILLONES TN', ' MILLONES TN'))
    trigo_anio_anterior = (listado_header[0], listado_anio_anterior[0], listado_anio_anterior[1].replace('MILLONES HA', ' MILLONES HA'), listado_anio_anterior[2].replace('QQ/HA', ' QQ/HA'), listado_anio_anterior[3].replace('MILLONES TN', ' MILLONES TN'))

########################################

    #Tabla del maiz
    maiz = estimaciones.find('table', class_ = 'bcr-estimaciones maiz color')

    listado_header_m = []
    cabecera_maiz = maiz.thead.find_all('th')
    for head in cabecera_maiz:
        listado_header_m.append(head.text)

    listado_ultimo_anio_m = []
    ultimo_anio_m = maiz.tbody.find_all('tr')[0]
    data = ultimo_anio_m.find_all('td')
    for d in data:
        listado_ultimo_anio_m.append(d.text)

    listado_anio_anterior_m = []
    ultimo_anio_m = maiz.tbody.find_all('tr')[1]
    data2 = ultimo_anio_m.find_all('td')
    for d2 in data2:
        listado_anio_anterior_m.append(d2.text)

    maiz_ultimo_anio = (listado_header_m[0], listado_ultimo_anio_m[0], listado_ultimo_anio_m[1].replace('MILLONES HA', ' MILLONES HA'), listado_ultimo_anio_m[2].replace('QQ/HA', ' QQ/HA'), listado_ultimo_anio_m[3].replace('MILLONES TN', ' MILLONES TN'))
    maiz_anio_anterior = (listado_header_m[0], listado_anio_anterior_m[0], listado_anio_anterior_m[1].replace('MILLONES HA', ' MILLONES HA'), listado_anio_anterior_m[2].replace('QQ/HA', ' QQ/HA'), listado_anio_anterior_m[3].replace('MILLONES TN', ' MILLONES TN'))

#################################
    ###### Tabla de la Soja
    soja = estimaciones.find('table', class_ = 'bcr-estimaciones soja color')

    listado_header_s = []
    cabecera_soja = soja.thead.find_all('th')
    for head in cabecera_soja:
        listado_header_s.append(head.text)

    listado_ultimo_anio_s = []
    ultimo_anio_s = soja.tbody.find_all('tr')[0]
    data = ultimo_anio_s.find_all('td')
    for d in data:
        listado_ultimo_anio_s.append(d.text)

    listado_anio_anterior_s = []
    ultimo_anio_s = soja.tbody.find_all('tr')[1]
    data2 = ultimo_anio_s.find_all('td')
    for d2 in data2:
        listado_anio_anterior_s.append(d2.text)

    soja_ultimo_anio = (listado_header_s[0], listado_ultimo_anio_s[0], listado_ultimo_anio_s[1].replace('MILLONES HA', ' MILLONES HA'), listado_ultimo_anio_s[2].replace('QQ/HA', ' QQ/HA'), listado_ultimo_anio_s[3].replace('MILLONES TN', ' MILLONES TN'))
    soja_anio_anterior = (listado_header_s[0], listado_anio_anterior_s[0], listado_anio_anterior_s[1].replace('MILLONES HA', ' MILLONES HA'), listado_anio_anterior_s[2].replace('QQ/HA', ' QQ/HA'), listado_anio_anterior_s[3].replace('MILLONES TN', ' MILLONES TN'))

#################################################################################
#########  BASE DE DATOS
    conexion = sqlite3.connect("agricultura_test.db")
    #Para crear una tabla, creamos una variable de tipo cursor
    cursor = conexion.cursor()

    cursor.execute(f"create table if not exists proyecciones_test (Cultivo VARCHAR(100), Periodo VARCHAR(100), {listado_header[1]} VARCHAR(100), {listado_header[2]} VARCHAR(100), {listado_header[3]} VARCHAR(100))")


    # Ingresar y leer varios registros al mismo tiempo
    cultivos_gral = [
        trigo_ultimo_anio,
        trigo_anio_anterior,
        maiz_ultimo_anio,
        maiz_anio_anterior,
        soja_ultimo_anio,
        soja_anio_anterior
    ]
    cursor.executemany("INSERT INTO proyecciones_test VALUES (?,?,?,?,?)", cultivos_gral)

    #Guardo los cambios
    conexion.commit()

    conexion.close()

############################################

def eliminar_datos_proyecciones():
        #### Borramos las filas de la tabla
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        cursor.execute('DELETE FROM proyecciones_test')
        #print('Se han eliminado', cursor.rowcount, 'filas de la tabla.')
        conexion.commit()
        conexion.close()

############################################

def consultar_trigo():
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        ##  print  trigos
        #cursor.execute('select * from proyecciones_test where (cultivo=:t)', {'t':'Trigo'})
        #busqueda = cursor.fetchall()
        #print("\nTabla de trigos: ")
        #for i in busqueda:
        #    print(i)
        print("\nTabla de proyecciones del Trigo:")
        df = pd.read_sql_query("select * from proyecciones_test where Cultivo = 'Trigo'", conexion)
        print(df.head())

        ##  print fila especifica: ultimo año
        cursor.execute('select * from proyecciones_test where (cultivo=:t and periodo=:p)', {'t':'Trigo','p':'2022/2023'})
        busqueda2 = cursor.fetchall()

        for item in busqueda2:
            for index,i in enumerate(item):
                if index == 2:
                    valor_actual_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_actual_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_actual_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))

        ##  print fila especifica: año anterior
        cursor.execute('select * from proyecciones_test where (cultivo=:t and periodo=:p)', {'t':'Trigo','p':'2021/2022'})
        busqueda3 = cursor.fetchall()

        for item in busqueda3:
            for index,i in enumerate(item):
                if index == 2:
                    valor_ant_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_ant_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_ant_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))

        conexion.close()
        ### CALCULAR VARIACION
        # variacion = ((val_actual - val_anterior) / val_anterior) * 100
        variacion_sembrado = ((valor_actual_sembrado - valor_ant_sembrado) / valor_ant_sembrado) * 100
        variacion_rinde = ((valor_actual_rinde - valor_ant_rinde) / valor_ant_rinde) * 100
        variacion_produccion = ((valor_actual_produccion - valor_ant_produccion) / valor_ant_produccion) * 100
        
        print("\n")
        print(f"Variación del area sembrada en base al anio anterior: {round(variacion_sembrado, 1)}%\n")
        print(f"Variación rindes en base al anio anterior: {round(variacion_rinde, 1)}%\n")
        print(f"Variación de la producción en base al anio anterior: {round(variacion_produccion, 1)}%\n")

############################################

def consultar_maiz():
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        ##  print  trigos
        """
        cursor.execute('select * from proyecciones_test where (cultivo=:m)', {'m':'Maiz'})
        busqueda = cursor.fetchall()
        print("\nTabla del maiz: ")
        for i in busqueda:
            print(i)
        """

        print("\nTabla de proyecciones del Maiz:")
        df = pd.read_sql_query("select * from proyecciones_test where Cultivo = 'Maiz'", conexion)
        print(df.head())

        ##  print fila especifica: ultimo año
        cursor.execute('select * from proyecciones_test where (cultivo=:m and periodo=:p)', {'m':'Maiz','p':'2022/2023'})
        busqueda2 = cursor.fetchall()

        for item in busqueda2:
            for index,i in enumerate(item):
                if index == 2:
                    valor_actual_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_actual_rinde = i.strip(" QQ/ MILLONES HA TA").replace(',','.')
                    if valor_actual_rinde == '':
                        valor_actual_rinde = float(0)
                    else:
                        valor_actual_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_actual_produccion = i.strip(" QQ/ MILLONES HA TA").replace(',','.')
                    if valor_actual_produccion == '':
                        valor_actual_produccion = float(0)
                    else:
                        valor_actual_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))


        ##  print fila especifica: año anterior
        cursor.execute('select * from proyecciones_test where (cultivo=:m and periodo=:p)', {'m':'Maiz','p':'2021/2022'})
        busqueda3 = cursor.fetchall()

        for item in busqueda3:
            for index,i in enumerate(item):
                if index == 2:
                    valor_ant_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_ant_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_ant_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))

        conexion.close()
        #### CALCULAR VARIACION
        # variacion = ((val_actual - val_anterior) / val_anterior) * 100
        
        if valor_actual_sembrado == 0.:
            print("Variación no disponible")
        else: 
            variacion_sembrado = ((valor_actual_sembrado - valor_ant_sembrado) / valor_ant_sembrado) * 100
            print("\n")
            print(f"Variación del area sembrada en base al anio anterior: {round(variacion_sembrado, 1)}%\n")

        if valor_actual_rinde == 0.:
            print("Variación rindes no disponible")
        else:
            variacion_rinde = ((valor_actual_rinde - valor_ant_rinde) / valor_ant_rinde) * 100
            print(f"Variación rindes en base al anio anterior: {round(variacion_rinde, 1)}%\n")

        if valor_actual_produccion == 0.:
            print("Variación de la producción no disponible")
        else: 
            variacion_produccion = ((valor_actual_produccion - valor_ant_produccion) / valor_ant_produccion) * 100
            print(f"Variación de la producción en base al anio anterior: {round(variacion_produccion, 1)}%\n")

############################################

def consultar_soja():
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        ##  print  trigos
        """
        cursor.execute('select * from proyecciones_test where (cultivo=:s)', {'s':'Soja'})
        busqueda = cursor.fetchall()
        print("\nTabla de la soja: ")
        for i in busqueda:
            print(i)
        """
        print("\nTabla de proyecciones de la Soja:")
        df = pd.read_sql_query("select * from proyecciones_test where Cultivo = 'Soja'", conexion)
        print(df.head())

        ##  print fila especifica: ultimo año
        cursor.execute('select * from proyecciones_test where (cultivo=:s and periodo=:p)', {'s':'Soja','p':'2022/2023'})
        busqueda2 = cursor.fetchall()

        for item in busqueda2:
            for index,i in enumerate(item):
                if index == 2:
                    valor_actual_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_actual_rinde = i.strip(" QQ/ MILLONES HA TA").replace(',','.')
                    if valor_actual_rinde == '':
                        valor_actual_rinde = float(0)
                    else:
                        valor_actual_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_actual_produccion = i.strip(" QQ/ MILLONES HA TA").replace(',','.')
                    if valor_actual_produccion == '':
                        valor_actual_produccion = float(0)
                    else:
                        valor_actual_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))

        ##  print fila especifica: año anterior
        cursor.execute('select * from proyecciones_test where (cultivo=:s and periodo=:p)', {'s':'Soja','p':'2021/2022'})
        busqueda3 = cursor.fetchall()

        for item in busqueda3:
            for index,i in enumerate(item):
                if index == 2:
                    valor_ant_sembrado = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 3:
                    valor_ant_rinde = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))
                elif index == 4:
                    valor_ant_produccion = float(i.strip(" QQ/ MILLONES HA TA").replace(',','.'))

        conexion.close()
        ### CALCULAR VARIACION
        # variacion = ((val_actual - val_anterior) / val_anterior) * 100
        
        if valor_actual_sembrado == 0.:
            print("Variación no disponible")
        else: 
            variacion_sembrado = ((valor_actual_sembrado - valor_ant_sembrado) / valor_ant_sembrado) * 100
            print("\n")
            print(f"Variación del area sembrada en base al anio anterior: {round(variacion_sembrado, 1)}%\n")

        if valor_actual_rinde == 0.:
            print("Variación rindes no disponible")
        else:
            variacion_rinde = ((valor_actual_rinde - valor_ant_rinde) / valor_ant_rinde) * 100
            print(f"Variación rindes en base al anio anterior: {round(variacion_rinde, 1)}%\n")

        if valor_actual_produccion == 0.:
            print("Variación de la producción no disponible")
        else: 
            variacion_produccion = ((valor_actual_produccion - valor_ant_produccion) / valor_ant_produccion) * 100
            print(f"Variación de la producción en base al anio anterior: {round(variacion_produccion, 1)}%\n")

#############################################

def cargar_datos_margenes():
    html_text = requests.get('https://inta.gob.ar/documentos/indicadores-economicos-e-informes-tecnicos').text
    #print(html_text) #response 200
    soup = BeautifulSoup(html_text, 'lxml')
    margenes = soup.find('div', class_ = 'panel-pane pane-views-panes pane-revision-contenido-documento-panel-pane-1 desarrollo-contenido no-title block')
    tabla_margenes= margenes.find('table')
    filas = tabla_margenes.find_all('tr')

    list_fila_1 = []
    list_fila_2 = []
    list_fila_3 = []
    list_fila_4 = []
    list_fila_5 = []
    list_fila_6 = []
    for index,fila in enumerate(filas):
        if index == 2:
            fila_1 = fila.find_all('td')
            for item in fila_1:
                valor = item.find('a')
                if valor is None:
                    valor = 'None'
                else:
                    valor = valor.text
                list_fila_1.append(valor)

        if index == 3:
            fila_2 = fila.find_all('td')
            for item in fila_2:
                valor2 = item.find('a').text
                list_fila_2.append(valor2)

        if index == 4:
            fila_3 = fila.find_all('td')
            for item in fila_3:
                valor3 = item.find('a').text
                list_fila_3.append(valor3)

        if index == 5:
            fila_4 = fila.find_all('td')
            for item in fila_4:
                valor4 = item.find('a').text
                list_fila_4.append(valor4)

        if index == 6:
            fila_5 = fila.find_all('td')
            for item in fila_5:
                valor5 = item.find('a').text.replace('\xa0', '')
                list_fila_5.append(valor5)
        
        if index == 7:
            fila_6 = fila.find_all('td')
            for item in fila_6:
                valor6 = item.find('a').text.replace('\xa0', '')
                list_fila_6.append(valor6)
    
    row1 = (list_fila_1[0],list_fila_1[1],list_fila_1[2], list_fila_1[3])
    row2 = (list_fila_2[0],list_fila_2[1],list_fila_2[2], list_fila_2[3])
    row3 = (list_fila_3[0],list_fila_3[1],list_fila_3[2], list_fila_3[3])
    row4 = (list_fila_4[0],list_fila_4[1],list_fila_4[2], list_fila_4[3])
    row5 = (list_fila_5[0],list_fila_5[1],list_fila_5[2], list_fila_5[3])
    row6 = (list_fila_6[0],list_fila_6[1],list_fila_6[2], list_fila_6[3])

    #######################################
    ##BASE DE DATOS
    conexion = sqlite3.connect("agricultura_test.db")
    #Para crear una tabla, creamos una variable de tipo cursor
    cursor = conexion.cursor()

    cursor.execute(f"create table if not exists margenes_test (Periodo VARCHAR(100), Elaboracion_estimada1 VARCHAR(100), Elaboracion_estimada2 VARCHAR(100), Obtenidos VARCHAR(100))")


    # Ingresar y leer varios registros al mismo tiemp5
    # 
    margenes_gral = [
        row1,
        row2,
        row3,
        row4,
        row5,
        row6
    ]
    cursor.executemany("INSERT INTO margenes_test VALUES (?,?,?,?)", margenes_gral)

    #Guardo los cambios
    conexion.commit()

    conexion.close()

#############################################

def eliminar_datos_margenes():
        #### Borramos las filas de la tabla
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        cursor.execute('DELETE FROM margenes_test')
        conexion.commit()
        conexion.close()

#############################################

def cotizacion_trigo():
    cotizaciones_trigo = pd.read_html('https://www.bolsadecereales.com')[0]

    ct = pd.DataFrame(cotizaciones_trigo)
    ct = ct.drop('Var', axis=1)
    ct = ct.drop('Posición', axis=1)
    ct = ct.drop(0, axis=0)
    return ct

#############################################

def cotizacion_maiz():
    cotizaciones_maiz = pd.read_html('https://www.bolsadecereales.com')[1]

    cm = pd.DataFrame(cotizaciones_maiz)
    cm = cm.drop('Var', axis=1)
    cm = cm.drop('Posición', axis=1)
    cm = cm.drop(0, axis=0)
    return cm

#############################################

def cotizacion_soja():
    cotizaciones_soja = pd.read_html('https://www.bolsadecereales.com')[2]

    cs = pd.DataFrame(cotizaciones_soja)
    cs = cs.drop('Var', axis=1)
    cs = cs.drop('Posición', axis=1)
    cs = cs.drop(0, axis=0)
    return cs

#############################################

def clearscreen():
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')  

def pressenter():
    print("*********************************************")
    print("Presione ENTER para continuar")
    input()
    clearscreen()
