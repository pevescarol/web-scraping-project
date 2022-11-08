from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd


##########################################
def cargar_datos_proyecciones():

    html_text = requests.get('https://www.bcr.com.ar/es/mercados/gea/estimaciones-nacionales-de-produccion/estimaciones').text
    #print(html_text) #response 200
    soup = BeautifulSoup(html_text, 'lxml')
    estimaciones = soup.find('div', class_ = 'table-estimaciones-responsive')

    #me trae todas las tablas de cada cultivo
    #cultivos = estimaciones.find_all('table', class_ = 'bcr-estimaciones')

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

    #print(trigo_anio_anterior)

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

    #######################################
    ##BASE DE DATOS
    conexion = sqlite3.connect("agricultura_test.db")
    #Para crear una tabla, creamos una variable de tipo cursor
    cursor = conexion.cursor()

    #chequeamos si existe la tabla
    print('Verificamos si la tabla ya existe')
    listTables = cursor.execute("select 'proyecciones_test' from sqlite_master where type='table'").fetchall()

    if listTables == []:
        print('...Creando tabla')
        #Creamos la tabla proyecciones
        cursor.execute(f"CREATE TABLE proyecciones_test (Cultivo VARCHAR(100), Periodo VARCHAR(100), {listado_header[1]} VARCHAR(100), {listado_header[2]} VARCHAR(100), {listado_header[3]} VARCHAR(100))")

    else:
        print('Tabla encontrada')

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

def eliminar_datos_proyecciones():
        #### Borramos las filas de la tabla
        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        cursor.execute('DELETE FROM proyecciones_test')
        #print('Se han eliminado', cursor.rowcount, 'filas de la tabla.')
        conexion.commit()
        conexion.close()

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



##########################################3


while True:
    print("*********************************************")
    cargar_datos_proyecciones()
    print("*********************************************\n")
    #####
    print('PROYECCIONES PARA LA PRODUCCION DE GRANOS EN ARGENTINA')
    print("""
\t 1 - Ver proyecciones de los granos en el último año
\t 2 - Ver proyecciones del trigo y su variacion
\t 3 - Ver proyecciones del maiz y su variacion
\t 4 - Ver proyecciones de la soja y su variacion
""")
    print('Ingresa la opción deseada: ')
    opcion = input('>')

    if opcion == '1':

        conexion = sqlite3.connect("agricultura_test.db")
        cursor = conexion.cursor()
        # haceoms consulta a la bd
        #for row in cursor.execute('select * from proyecciones_test'):
        #    print(row)

        df = pd.read_sql_query('select * from proyecciones_test', conexion)
        # Ver  el resultado de la consulta SQL está
        # almacenado en el DataFrame
        print(">>> Proyecciones de la producción de granos: ")
        print(df.head())
        eliminar_datos_proyecciones()
    
    elif opcion == '2':
        consultar_trigo()
        eliminar_datos_proyecciones()

    elif opcion == '3':
        consultar_maiz()
        eliminar_datos_proyecciones()

    elif opcion == '4':
        consultar_soja()
        eliminar_datos_proyecciones()

    else:
        print('Hasta luego!')
        eliminar_datos_proyecciones()
        break


