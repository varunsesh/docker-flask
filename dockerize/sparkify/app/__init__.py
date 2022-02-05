from flask import Flask

spark_app = Flask(__name__)

def create_app():
    spark_app.run(host='0.0.0.0', debug=True)


    