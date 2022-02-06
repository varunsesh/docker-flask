import pandas as pd
 
# to read csv file named "sample"
def csv2html(filename):
    #pandas reads the csv file
    a = pd.read_csv(filename)
    
    # to save as html file
    # named as "Table"
    a.to_html("Test.html")
    
    # assign it to a
    # variable (string)
    html_file = a.to_html()

    return(html_file)