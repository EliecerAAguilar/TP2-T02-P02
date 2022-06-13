import pandas as pd
import os
import numpy as np
import psycopg2

data = pd.read_csv(r'resources/babynames_births2021.csv', sep='\t')
df = pd.DataFrame(data)
# print(df)
a = df['Rank']
b = df['Male_name']
c = df['Female_name']


class Rege:
    def __init__(self):
        self.connection = psycopg2.connect(host="localhost", dbname="babynames.sql", user="postgres",
                                           password="erod1097")
        self.micursor = self.connection.cursor()

    def insertarRege(self, rango, nombremasc, nombrefem):
        for x in range(len(a)):
            query = ("INSERT INTO db_babynames values(%s,%s,%s)")
            val = (str(rango[x]), str(nombremasc[x]), str(nombrefem[x]))
            self.micursor.execute(query, val)
        print("OHIO!")
        try:
            # self.connection.commit()
            print("Se insertó correctamente")
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
            print("Error al insertar registro")
        finally:
            if self.connection:
                # Los 100 primeros nombres mas populares (para ambos géneros).
                SQL1 = "SELECT nombre_masc, nombre_fem FROM db_babynames WHERE rango <=100 ORDER BY rango;"
                self.micursor.execute(SQL1)
                resultados = self.micursor.fetchall()
                print('valores: ', resultados)
                # Todos los nombres que tienen 4 letras o menos y sus rangos(para ambos géneros).
                SQL2 = "SELECT * FROM db_babynames WHERE (char_length(nombre_masc)<=4 AND char_length(nombre_fem)<=4);"
                self.micursor.execute(SQL2)
                resultados = self.micursor.fetchall()
                print('valores: ', resultados)
                # Imprima todos los nombres femeninos que tengan las letras ‘w’ o ‘x’ o ‘y’ o ‘z’ en ellas.
                SQL3 = "SELECT nombre_fem FROM db_babynames WHERE (nombre_fem LIKE('%w%')) OR (nombre_fem LIKE('%x%')) OR (nombre_fem LIKE('%y%')) OR (nombre_fem LIKE('%z%'));"
                self.micursor.execute(SQL3)
                resultados = self.micursor.fetchall()
                print('valores: ', resultados)
                # Todos los nombres que tienen dos letras repetidas (de 3 casos distintos), por ejemplo: ‘aa’, ‘cc’, ‘pp’ (la letra p seguido de p).
                SQL4 = "SELECT nombre_masc FROM db_babynames WHERE (nombre_masc LIKE('%ii%')) OR (nombre_masc LIKE('%ll%')) OR (nombre_masc LIKE('%nn%')) UNION ALL SELECT nombre_fem FROM db_babynames WHERE (nombre_fem LIKE('%ii%')) OR (nombre_fem LIKE('%ll%')) OR (nombre_fem LIKE('%nn%'));"
                self.micursor.execute(SQL4)
                # Resultados
                resultados = self.micursor.fetchall()
                print('valores: ', resultados)
                self.connection.close()
                self.micursor.close()
                print("Se cerró la conexión")


objRege = Rege()
objRege.insertarRege(a, b, c)

"""file_name1 = 'C:/Users/efige/Documents/Portafolio 2021/I SEM/TÓPICOS ESPECIALES II/Tareas/Tarea2/archivos_tarea2/01/names2018.txt'
with open(file_name1) as file_object1:
    #lines = file_object1.readlines()
#for line in lines:
#    print (line.rstrip())
    for line in file_object1:
        print(line.rstrip())"""

"""file_name2 = 'C:/Users/efige/Documents/Portafolio 2021/I SEM/TÓPICOS ESPECIALES II/Tareas/Tarea2/archivos_tarea2/01/names2018.txt'
with open(file_name2) as file_object2:
    contents = file_object2.read()
    print(contents.rstrip())"""


class Rege:
    def __init__(self):
        self.connection = psycopg2.connect(host="localhost", dbname="babynames", user="postgres",
                                           password="erod1097")
        self.micursor = self.connection.cursor()

    def insertarRege(self, id, rango, nombre, sexo, num_naci):
        query = "INSERT INTO nombebe VALUES(%s,%s,%s,%s,%s)"
        val = (id, rango, nombre, sexo, num_naci)
        try:
            self.micursor.execute(query, val)
            self.connection.commit()
            print("Se insertó correctamente")
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
            print("Error al insertar registro")
        finally:
            if self.connection:
                self.connection.close()
                self.micursor.close()
                print("Se cerró la conexión")


objRege = Rege()
objRege.insertarRege(1, 1, "Liam", "M", 19837)

"""-------------------------------------------------------------------------------------"""
#def main():


# if __name__ == '__main__':
#     main()

