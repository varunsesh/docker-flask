from flask import redirect, request, render_template, url_for
from app import spark_app
import pickle

#A global variable that stores the value of the customer ID. This is a hack, needs to be fixed, but it works for now.
ID=0


@spark_app.route("/", methods=["GET", "POST"])
def index():
    global ID
    if request.method=="POST":
        customerID=request.form["cid"]
        ID=int(customerID)
        return redirect(url_for('display', custID=customerID))
    else:
        ID=0
        return render_template("index.html")
    

@spark_app.route("/display/<custID>", methods=["GET","POST"])
def display(custID):
    #This ID is essentially an an integer from 0 to 299: Here the assumption is the customer ID is 
    # an index of the test data. The loan officer who supplies the role can get the details of the applicant and also run the risk assessment themself.
    #The fetch raw data has not been implemented here. 
    custID=custID
    if request.method=="GET":
        #retval = utils.evaluate_risk(custID)
        return render_template("display.html", customerID=custID)
    
    return render_template(url_for('record'), request.form("response"), custID)
    

@spark_app.route("/response", methods=["POST"])
def record():
    global ID
    response = request.get_data().decode("utf-8")
    response = response.replace("response=", "")
    if response == "Accept":
        value = 0
    else:
        value = 1
    utils.record_response(ID, value) 
    print(utils.new_data)
    return render_template("response.html")

