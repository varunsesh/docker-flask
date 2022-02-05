from flask import redirect, request, render_template, url_for
from app import spark_app, ml_model, utils
import pickle

#A global variable that stores the value of the customer ID. This is a hack, needs to be fixed, but it works for now.


@spark_app.route("/", methods=["GET", "POST"])
def index():
    
    if request.method=="POST":
        return redirect(url_for('display'))
    else:
        return render_template("index.html")
    

@spark_app.route("/display", methods=["GET","POST"])
def display():
    ml_model.compute_model()
    html_script = utils.csv2html("submission.csv")
    return render_template("display.html", title="Evaluate", html_script=html_script)

    