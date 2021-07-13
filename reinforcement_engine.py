# start reinforcement_learning_engine

from flask import Flask, request, make_response
import dynamic_database_manage as dbHandler
import json
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'application/json'

# method to check server RL server is running
@app.route('/home')
def serve():
    return 'This page is served via Flask!'

# @app.after_request
# def after_request(response):
#     response.headers.add('Access-Control-Allow-Origin', '*')
#     response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
#     response.headers.add('Access-Control-Allow-Methods', 'GET,POST') # Put any other methods you need here
#     response.headers['X-UA-Compatible'] = 'IE=Edge,chrome=1'
#     response.headers['Cache-Control'] = 'public, max-age=0'
#     return response

# Upgrade product weight
@app.route('/reward', methods=['POST'])
def reward_operation():

    if request.method=='POST':
        # Object of database handler
        db_obj = dbHandler.DbController()
        data = request.get_json(silent=True)
        list_of_tuple = (data['URL'],data['Product_Title'],data['tag'],data['Product_Price'],data['Product_Volume'],data['price_per_base_volume'],data['Category'],data['Product_Detail'],data['Ingredients'],data['Nutritional_information'],data['Allergen_warnings'],data['Claims'],data['Endorsements'],data['Product_Image'],data['Product_origin'])
        # print(list_of_tuple)
        out = db_obj.reward_product(list_of_tuple)
        if out == True:
            response = make_response(json.dumps({
                'Status': out
            }))
        else:
            response = make_response(json.dumps({
                'Status': out
            }))
        return response
    else:
        return None

# downgrade product weight
@app.route('/feedback', methods=['POST'])
def feedback_operation():
    if request.method=='POST':
        # Object of database handler
        db_obj = dbHandler.DbController()
        data = request.get_json(silent=True)
        list_of_tuple = (data['URL'],data['Product_Title'],data['tag'],data['Product_Price'],data['Product_Volume'],data['price_per_base_volume'],data['Category'],data['Product_Detail'],data['Ingredients'],data['Nutritional_information'],data['Allergen_warnings'],data['Claims'],data['Endorsements'],data['Product_Image'],data['Product_origin'])
        # print(list_of_tuple)
        out = db_obj.feedback_product(list_of_tuple)
        if out == True:
            response = make_response(json.dumps({
                'Status': out
            }))
        else:
            response = make_response(json.dumps({
                'Status': out
            }))
        return response
    else:
        return None

@app.after_request
def add_headers(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    return response

app.run(host= '0.0.0.0', port=5000)
