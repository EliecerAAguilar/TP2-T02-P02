import pandas as pd
import psycopg2


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
        self.id = "id"
        self.rango = "rango"
        self.nombre = "nombre"
        self.sexo = "sexo"
        self.numero_de_nacimientos = "num_nac"

        self.connection = psycopg2.connect(host=self.host, dbname=self.database, user=self.user, port=self.port,
                                           password=self.pwd)
        self.connection.autocommit = True
        self.micursor = self.connection.cursor()
        # self.micursor.execute("""CREATE DATABASE babynames;""")
        # self.micursor.execute("""GRANT ALL PRIVILEGES ON DATABASE babynames TO test;""")
        """aqui nos aseguramos que la tabla se elimine antes de hacer las inserciones para no duplicar valores"""
        self.micursor.execute("""DROP TABLE IF EXISTS babyname;""")
        self.column_names = (self.id, self.rango, self.nombre, self.sexo, self.numero_de_nacimientos)
        self.micursor.execute("""CREATE TABLE IF NOT EXISTS babyname(%s SERIAL PRIMARY KEY, %s INTEGER, \
         %s VARCHAR(150), %s CHAR(1)), %s INTEGER""", self.column_names)
        self.connection.close()
        self.database = "babynames"
        self.connection = psycopg2.connect(host=self.host, dbname=self.database, user=self.user, port=self.port,
                                           password=self.pwd)
        self.connection.autocommit = True
        # self.connection.commit()

    def insert_reg(self, men, women):

        try:
            """insertando registro de nacimientos de los varones"""
            query = """INSERT INTO babynames(rango,nombre,sexo,num_nac) values(%s, %s, %s, %s)"""
            for i, row in men.iterrows():
                self.micursor.execute(query, tuple(row))

            """insertando registro de nacimientos de las mujeres"""
            query = """INSERT INTO babynames(rango,nombre,sexo,num_nac) values(%s, %s, %s, %s)"""
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
                query01 = """SELECT nombre FROM babynames WHERE sexo LIKE 'M' LIMIT 100"""
                self.micursor.execute(query01)
                resultados = self.micursor.fetchall()
                print("100 primeros nombres mas populares masculinos")
                print(resultados)
                query01 = "SELECT nombre FROM babynames WHERE sexo LIKE 'F' LIMIT 100"""
                self.micursor.execute(query01)
                resultados = self.micursor.fetchall()
                print("100 primeros nombres mas populares femeninos")
                print(resultados)

                """Todos los nombres que tienen 4 letras o menos y sus rangos(para ambos géneros)"""
                query02 = """SELECT rango, nombre FROM babynames WHERE (char_length(nombre)<=4\
                 AND sexo LIKE 'M');"""
                self.micursor.execute(query02)
                resultados = self.micursor.fetchall()
                print("nombres masculinos de 4 letras o menos")
                print(resultados)
                query02 = """SELECT rango, nombre FROM babynames WHERE (char_length(nombre)<=4\
                 AND sexo LIKE 'F');"""
                self.micursor.execute(query02)
                resultados = self.micursor.fetchall()
                print("nombres femeninos de 4 letras o menos")
                print(resultados)

                """Imprima todos los nombres femeninos que tengan las letras ‘w’ o ‘x’ o ‘y’ o ‘z’ en ellas."""
                query03 = """SELECT nombre FROM babynames WHERE (\
                (\
                    (nombre LIKE('%w%')) \
                    OR (nombre LIKE('%x%'))\
                    OR (nombre LIKE('%y%'))\
                    OR (nombre LIKE('%z%'))\
                )\
                AND (sexo LIKE 'F')\
                );"""
                self.micursor.execute(query03)
                resultados = self.micursor.fetchall()
                print("Nombres femeninos con letras 'w', 'x','y' ó 'z'")
                print(resultados)

                """d. Todos los nombres que tienen dos letras repetidas (de 3 casos distintos), por ejemplo: ‘aa’, ‘tt,
                 ‘pp’ (la letra p seguido de p)"""
                query04 = """SELECT nombre FROM babynames WHERE (nombre LIKE('%ii%')) \
                OR (nombre LIKE('%ll%'))\
                OR (nombrec LIKE('%nn%')) UNION ALL SELECT nombre FROM babynames \
                WHERE (nombre LIKE('%ii%'))\
                OR (nombre LIKE('%ll%')) OR (nombre LIKE('%nn%'));"""
                self.micursor.execute(query04)
                # Resultados
                resultados = self.micursor.fetchall()
                print('valores: ', resultados)
                # self.connection.commit
                self.micursor.close()
                self.connection.close()

                print("Se cerró la conexión")


"""-------------------------------------------------------------------------------------"""


def main():
    try:
        data = pd.read_csv(r'resources/babynames_births2021.csv')
        df = pd.DataFrame(data)

        """copiando los valores del dataframe original y separandolos por sexo usando la funcion .loc """

        males = df.loc[:, ["Rank", "Male name", "Number of males"]]
        males["sexo"] = 'M'

        females = df.loc[:, ["Rank", "Female name", "Number of females"]]
        females["sexo"] = 'F'

        registrar = Registro()
        registrar.insert_reg(males, females)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()

