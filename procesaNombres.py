import pandas as pd
import psycopg2
import prettytable as pt


class Registro:

    def __init__(self):

        """se debe conceder los permisos para la creacion de base de datos en el servidor de postgres"""
        """alter role <user_name> with createdb"""
        """o"""
        """alter role <user_name> with superuser"""

        """datos para la conexion a la base de datos"""
        self.port = '5433'
        self.database = "test"
        self.host = "localhost"
        self.user = "test"
        self.pwd = "test-pass"

        """nombre de las columnas de la tabla babyname"""
        """
            self.id = "id"
            self.rango = "rango"
            self.nombre = "nombre"
            self.sexo = "sexo"
            self.numero_de_nacimientos = "num_nac"
        """
        # self.connection = psycopg2.connect(host=self.host, dbname=self.database, user=self.user, port=self.port,
        #                                    password=self.pwd)
        # self.connection.autocommit = True
        # self.micursor = self.connection.cursor()
        # self.micursor.execute("""CREATE DATABASE babynames;""")
        # self.micursor.execute("""GRANT ALL PRIVILEGES ON DATABASE babynames TO test;""")

        self.database = "babynames"
        self.connection = psycopg2.connect(host=self.host, dbname=self.database, user=self.user, port=self.port,
                                           password=self.pwd)
        self.connection.autocommit = True
        self.micursor = self.connection.cursor()

        """aqui nos aseguramos que la tabla se elimine antes de hacer las inserciones para no duplicar valores"""
        self.micursor.execute("""DROP TABLE IF EXISTS babyname;""")
        # self.column_names = (self.id, self.rango, self.nombre, self.sexo, self.numero_de_nacimientos)
        self.micursor.execute("""CREATE TABLE IF NOT EXISTS babyname(id SERIAL PRIMARY KEY, rango INTEGER, \
         nombre VARCHAR(150), sexo CHAR(1), num_nac INTEGER);""")
        # self.connection.close()
        # self.connection.commit()

    def insert_reg(self, men, women):

        try:
            """insertando registro de nacimientos de los varones"""
            query = """INSERT INTO babyname(rango,nombre,num_nac,sexo) values(%s, %s, %s, %s)"""
            for i, row in men.iterrows():
                self.micursor.execute(query, tuple(row))

            """insertando registro de nacimientos de las mujeres"""
            query = """INSERT INTO babyname(rango,nombre,num_nac,sexo) values(%s, %s, %s, %s)"""
            for i, row in women.iterrows():
                self.micursor.execute(query, tuple(row))

            # self.connection.commit()
            print("Se insertó correctamente")
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
            print("Error al insertar registro")
        finally:
            if self.connection:
                """Los 100 primeros nombres mas populares (para ambos géneros)"""
                """WHERE rango <=100 ORDER BY rango;"""
                query01 = """SELECT nombre FROM babyname WHERE sexo LIKE 'M' LIMIT 100"""
                self.micursor.execute(query01)
                resultados = pt.from_db_cursor(self.micursor)
                # resultados = self.micursor.fetchall()
                print("100 primeros nombres mas populares masculinos")
                print(resultados)
                query01 = "SELECT nombre FROM babyname WHERE sexo LIKE 'F' LIMIT 100"""
                self.micursor.execute(query01)
                # resultados = self.micursor.fetchall()
                resultados = pt.from_db_cursor(self.micursor)
                print("100 primeros nombres mas populares femeninos")
                print(resultados)

                """Todos los nombres que tienen 4 letras o menos y sus rangos(para ambos géneros)"""
                query02 = """SELECT rango, nombre FROM babyname WHERE (char_length(nombre)<=4\
                 AND sexo LIKE 'M');"""
                self.micursor.execute(query02)
                # resultados = self.micursor.fetchall()
                resultados = pt.from_db_cursor(self.micursor)
                print("nombres masculinos de 4 letras o menos")
                print(resultados)
                query02 = """SELECT rango, nombre FROM babyname WHERE (char_length(nombre)<=4\
                 AND sexo LIKE 'F');"""
                self.micursor.execute(query02)
                # resultados = self.micursor.fetchall()
                resultados = pt.from_db_cursor(self.micursor)
                print("nombres femeninos de 4 letras o menos")
                print(resultados)

                """Imprima todos los nombres femeninos que tengan las letras ‘w’ o ‘x’ o ‘y’ o ‘z’ en ellas."""
                query03 = """SELECT nombre FROM babyname WHERE (\
                (\
                    (nombre LIKE('%w%')) \
                    OR (nombre LIKE('%x%'))\
                    OR (nombre LIKE('%y%'))\
                    OR (nombre LIKE('%z%'))\
                )\
                AND (sexo LIKE 'F')\
                );"""
                self.micursor.execute(query03)
                # resultados = self.micursor.fetchall()
                resultados = pt.from_db_cursor(self.micursor)
                print("Nombres femeninos con letras 'w', 'x','y' ó 'z'")
                print(resultados)

                """d. Todos los nombres que tienen dos letras repetidas (de 3 casos distintos), por ejemplo: ‘aa’, ‘tt,
                 ‘pp’ (la letra p seguido de p)"""
                # query04 = """SELECT nombre FROM babynames WHERE (nombre LIKE('%ii%')) \
                # OR (nombre LIKE('%ll%'))\
                # OR (nombrec LIKE('%nn%')) UNION ALL SELECT nombre FROM babynames \
                # WHERE (nombre LIKE('%ii%'))\
                # OR (nombre LIKE('%ll%')) OR (nombre LIKE('%nn%'));"""
                # query04 = r"""SELECT regexp_matches(nombre,'(\w)\1') FROM babyname;"""
                query04 = r"""SELECT nombre FROM babyname WHERE nombre ~ '(\w)\1'"""
                self.micursor.execute(query04)
                # resultados = self.micursor.fetchall()
                resultados = pt.from_db_cursor(self.micursor)
                print("Nombres con dos letras repetidas")
                print(resultados)
                # self.connection.commit
                self.micursor.close()
                self.connection.close()

                # print("Se cerró la conexión")


"""------------------------------------------------------------------------------------------------------------------"""


def main():
    try:
        data = pd.read_csv(r'resources/babynames_births2021.csv')
        df = pd.DataFrame(data)

        """copiando los valores del dataframe original y separandolos por sexo usando la funcion .loc """

        males = df.loc[:, ["Rank", "Male name", "Number of males"]]
        males["sexo"] = 'M'

        females = df.loc[:, ["Rank", "Female name", "Number of females"]]
        females["sexo"] = 'F'

        males["Number of males"] = males["Number of males"].apply(lambda x: x.replace(',', ''))
        females["Number of females"] = females["Number of females"].apply(lambda x: x.replace(',', ''))
        # print(females.head())
        # print(males.head())
        registrar = Registro()
        registrar.insert_reg(males, females)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()

