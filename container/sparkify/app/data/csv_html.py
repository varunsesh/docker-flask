
 
import pandas as pd
 
# to read csv file named "samplee"
a = pd.read_csv("test.csv")
 
# to save as html file
# named as "Table"
a.to_html("Test.html")
 
# assign it to a
# variable (string)
html_file = a.to_html()

print(html_file)