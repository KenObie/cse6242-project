import sqlalchemy
import pandas as pd


class DataLoader:
    def __init__(self, host: str, user: str, passwd: str, db: str):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db

    def push_data(self, filename: str, table_name: str, index: bool):
        # load data to dataframe
        print(filename)

        data = pd.read_csv(filename)
        df = pd.DataFrame(data)

        # format connection string
        connection_string = 'mysql+mysqldb://{user}:{passwd}@{host}/{database}'.format(user=self.user,
                                                                                       passwd=self.passwd,
                                                                                       host=self.host,
                                                                                       database=self.db,
                                                                                       )
        # connect to mysql database
        connection = sqlalchemy.create_engine(connection_string)

        # truncate database ?? we can change this if we think it will be an issue
        truncate_query = "TRUNCATE TABLE {table}".format(table=table_name)
        connection.execute(truncate_query)

        # push data to sql db
        df.to_sql(con=connection, name=table_name, if_exists='append', index=index)
