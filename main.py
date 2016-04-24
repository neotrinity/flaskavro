from flask import Flask, Response
from avro_util import gen_avro
from os import urandom, SEEK_SET
import fastavro
import io


app = Flask(__name__)


def get_data():
    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
        ],
    }

    records = [
        {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
        {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
        {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
        {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    ]*100000
    
    return (schema, records)


@app.route('/aaa.avro')
def generated_avro():
    schema, records = get_data()
    gen = gen_avro(schema, records)
    return Response(gen, mimetype='application/octet-stream')


@app.route('/bbb.avro')
def get_avro():
    schema, records = get_data()
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return Response(buf.getvalue(), mimetype='application/octet-stream')

if __name__ == '__main__':
    app.run(debug=True)