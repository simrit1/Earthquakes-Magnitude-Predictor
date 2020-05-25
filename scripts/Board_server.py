"""
	DASHBOARD SERVER
"""

from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')

def index():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(
        port  = 5001, 
        debug = True
    )