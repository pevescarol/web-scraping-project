import pandas as pd
from funciones import cotizacion_maiz, cotizacion_soja, cotizacion_trigo


## Para guardar sacar los COMENTARIOS y ejecutar !!!

#### IMPRIMIR ESTIMACIONES DE LOS GRANOS ####

url_trigo = 'https://www.bcr.com.ar/es/mercados/gea/estimaciones-nacionales-de-produccion/estimaciones'
est_trigo = pd.read_html(url_trigo)[0]
df_t = pd.DataFrame(est_trigo)
#df_t.to_csv('estimaciones_trigo.csv', index = False)

url_maiz = 'https://www.bcr.com.ar/es/mercados/gea/estimaciones-nacionales-de-produccion/estimaciones'
est_maiz = pd.read_html(url_maiz)[1]
df_m = pd.DataFrame(est_maiz)
#df_m.to_csv('estimaciones_maiz.csv', index = False)

url_soja = 'https://www.bcr.com.ar/es/mercados/gea/estimaciones-nacionales-de-produccion/estimaciones'
est_soja = pd.read_html(url_soja)[2]
df_s = pd.DataFrame(est_soja)
#df_s.to_csv('estimaciones_soja.csv', index = False)

#################################################

#### IMPRIMIR MARGENES DE LOS GRANOS ####

url_margenes = 'https://inta.gob.ar/documentos/indicadores-economicos-e-informes-tecnicos'

margenes_total = pd.read_html(url_margenes)[0]
mg = pd.DataFrame(margenes_total)
#mg.to_csv('margenes_total.csv', index = False)


#################################################

#### IMPRIMIR LAS COTIZACIONES ####

#cotizacion_trigo().to_csv('cot_trigo.csv', index = False)

#cotizacion_maiz().to_csv('cot_maiz.csv', index = False)

#cotizacion_soja().to_csv('cot_soja.csv', index = False)





