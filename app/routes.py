from flask import render_template, request
from werkzeug.utils import secure_filename
import random
import string


from . import application


def generate_bogus_results(size):
    results = []
    for i in range(size):
        pdbid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        chainid = random.choice(string.ascii_uppercase)
        qscore = random.random()

        results.append((f'{pdbid}:{chainid}', qscore))

    return sorted(results, key=lambda x: x[1], reverse=True)


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        chain = request.form['chain'].upper()
        file = request.files['file']
        filename = secure_filename(file.filename).upper()
        file.save(f'/tmp/{filename}')

        results = generate_bogus_results(random.randint(1, 10))
        query = f'{filename}:{chain}'

        return render_template('results.html', query=query, results=results)

    return render_template('index.html')


