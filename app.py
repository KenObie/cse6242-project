from common.data.data_loader import DataLoader
from flask import Flask
from flask_mysqldb import MySQL

app = Flask(__name__)

host = "localhost"
user = "root"
passwd = ""
database = "cse6242"
geo_ids_file = "data/GeoIDCounty.csv"
geo_id_table = "geo_ids"

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'cse6242'

fetch_data = DataLoader(host, user, passwd, database).push_data(geo_ids_file, geo_id_table, index=False)

mysql = MySQL(app)


@app.route('/')
def hello_world():
    return 'Not hello world'


@app.route('/geo-ids', methods=['GET'])
def geo_ids():
    cursor = mysql.connection.cursor()
    cursor.execute("SELECT * FROM geo_ids")
    field_names = [i[0] for i in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    print(data)
    return {
        'title': 'GeoIds',
        'columns': field_names,
        'data': data
    }


if __name__ == '__main__':
    app.run()
