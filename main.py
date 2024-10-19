import mysql.connector
from flask import Flask, request, jsonify
from mysql.connector import Error

app = Flask(__name__)

# Conexión a MySQL
def get_db_connection():
    conn = mysql.connector.connect(
        host='localhost',
        database='data_covid',
        user='root',
        password='',
    )
    return conn

# READ - Obtener todos los registros o uno específico por UUID
@app.route('/fallecidos', methods=['GET'])
def get_fallecidos():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM fallecidos_covid")
        registros = cursor.fetchall()
        return jsonify(registros), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# READ - Obtener un registro por UUID
@app.route('/fallecidos/<uuid>', methods=['GET'])
def obtener_fallecido(uuid):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT * FROM fallecidos_covid WHERE uuid = %s"
    cur.execute(query, (uuid,))
    row = cur.fetchone()
    if row:
        fallecido = {
            "uuid": row[0], "fecha_fallecimiento": row[1], "edad_declarada": row[2], 
            "sexo": row[3], "fecha_nac": row[4], "departamento": row[5], 
            "provincia": row[6], "distrito": row[7]
        }
        cur.close()
        conn.close()
        return jsonify(fallecido)
    else:
        cur.close()
        conn.close()
        return jsonify({"message": "Registro no encontrado"}), 404

# UPDATE - Actualizar un registro por UUID
@app.route('/fallecidos/<uuid>', methods=['PUT'])
def actualizar_fallecido(uuid):
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        UPDATE fallecidos_covid
        SET fecha_fallecimiento = %s, edad_declarada = %s, sexo = %s, fecha_nac = %s,
            departamento = %s, provincia = %s, distrito = %s
        WHERE uuid = %s
    """
    cur.execute(query, (
        data['fecha_fallecimiento'], data['edad_declarada'], data['sexo'], 
        data['fecha_nac'], data['departamento'], data['provincia'], 
        data['distrito'], uuid
    ))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Registro actualizado con éxito"}), 200

# DELETE - Eliminar un registro por UUID
@app.route('/fallecidos/<uuid>', methods=['DELETE'])
def eliminar_fallecido(uuid):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "DELETE FROM fallecidos_covid WHERE uuid = %s"
    cur.execute(query, (uuid,))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Registro eliminado con éxito"}), 200

# Ejecutar la aplicación de Flask
if __name__ == '__main__':
    app.run(debug=True)
