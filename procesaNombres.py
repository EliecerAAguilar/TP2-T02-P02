import pandas as pd
import psycopg2


class Registro:

    def __init__(self):
        self.connection = psycopg2.connect(host="localhost", dbname="babynames.sql", user="test", port=5433,
                                           password="test-pass")
        self.micursor = self.connection.cursor()
        self.micursor.execute("""CREATE DATABASE babynames;""")
        self.micursor.execute("""GRANT ALL PRIVILEGES ON DATABASE babynames TO test;""")
        self.connection.commit()

    def insertarRege(self, rango, nombremasc, nombrefem):

        try:
            for x in range(len(rango)):
                query = ("INSERT INTO db_babynames values(%s,%s,%s)")
                val = (str(rango[x]), str(nombremasc[x]), str(nombrefem[x]))
                self.micursor.execute(query, val)

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
                self.connection.commit()
                self.connection.close()
                self.micursor.close()
                print("Se cerró la conexión")


"""-------------------------------------------------------------------------------------"""


def main():
    data = pd.read_csv(r'resources/babynames_births2021.csv', sep='\t')
    df = pd.DataFrame(data)

    rank = df['Rank']
    male_names = df['Male name']
    female_names = df['Female name']
    registrar = Registro()
    registrar.insertarRege(rank, male_names, female_names)


if __name__ == '__main__':
    main()

