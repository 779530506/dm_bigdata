# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from apps.home import blueprint
from flask import render_template, request,flash,redirect,url_for
from flask_login import login_required
from jinja2 import TemplateNotFound
import os
from apps.home.utils import getData,getDataSearch,getFields,getSearchMultiple,process_csv
from datetime import datetime
import pandas as pd

@blueprint.route('/index')
@login_required
def index():
    try:
        data = getData()
        fields = getFields()
        personnes = data["hits"]["hits"]
        total = data["hits"]["total"]
        nbrPersonne = len(personnes)
        return render_template('home/index.html', total=total,segment='index',fields=fields,personnes=personnes,nbr=nbrPersonne)
    except Exception as e:
        return str(e)

@blueprint.route('/imports')
def imports():
    return render_template('home/import.html', segment='imports')

ALLOWED_EXTENSIONS = set(['csv','txt'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@blueprint.route('/upload', methods=['GET', 'POST'])
def upload_file():
    fields = getFields()
    if request.method == 'POST':
        file = request.files['file']
        delimiter=request.values['delimiter']
        if file and allowed_file(file.filename):
        #return f'uploaded {f.filename}'
            rep ="/home/abdoulayesarr/Documents/Digital_management/tmp"
            #rep ="/home/data/Documents/dm/tmp"
            new_filename = f'{file.filename.split(".")[0]}_{str(datetime.now())}.csv'
            file.save(os.path.join(rep,new_filename))
            #output = process_csv(os.path.join(rep,new_filename))
            filename=os.path.join(rep,new_filename)
            #with open(os.path.join(rep,new_filename)) as file:
            data = pd.read_csv(filename,header=0,sep=delimiter) 
            colonnes=[]
            
            myData = data.values[:5]
            for row in data.columns:
                colonnes.append(row)
          
            nbrColonne = len(colonnes)
            return render_template("home/s3_csv_table.html",segment='imports',filename=filename,delimiter=delimiter, fields=fields,nbrColonne=nbrColonne,myData=myData,colonnes=colonnes)
            
        else:
            flash('format de fichier incorrect','danger')
            #return redirect(url_for('home_blueprint.upload_file'))
            return render_template('home/import.html')

@blueprint.route('/saved_file', methods=['GET', 'POST'])
def saved_file():
    if request.method == 'POST':
        file = request.values['filename']
        sep = request.values['sep']
        header = request.values['header']
        colonnes= request.values["hidden_colonnes"].split(",")
        cols= request.form.getlist('mytext[]')
        #if(header=="oui"):
        try:
            process_csv(file,sep,colonnes,header,cols)
            flash('file loaded successful','success')
            return render_template('home/import.html',segment='imports',)
        except:
            flash('Erreur, file error','danger')
            return render_template('home/import.html',segment='imports',)

    return request.values

@blueprint.route('/search',methods=['GET', 'POST'])
def search():
    try:
        colonnes,value = request.values["hidden_colonnes"].split(","),request.values["value"]
        if request.values["value"]=='':
            return redirect(url_for('home_blueprint.index'))
        
        fields = getFields()
        data = getDataSearch(colonnes,value)
        total = data["hits"]["total"]
        personnes = data["hits"]["hits"]
        nbrPersonne = len(personnes)
        return render_template('home/index.html',total=total, segment='index',fields=fields,personnes=personnes,nbr=nbrPersonne)
    except Exception as e:
        return str(e)

@blueprint.route('/search_by_colonne')
def searchByColonne():
    # try:
    #     colonnes,value = request.values["hidden_colonnes"].split(","),request.values["value"]
    #     if request.values["value"]=='':
    #         return redirect(url_for('home_blueprint.index'))
        
    #     fields = getFields()
    #     data = getDataSearch(colonnes,value)
    #     personnes = data["hits"]["hits"]
    #     nbrPersonne = len(personnes)
    fields = getFields()
    return render_template('home/search.html', segment='searchmultiple',nbr=0)
    # except Exception as e:
    #     return str(e)
@blueprint.route('/searchmultiple',methods=['GET', 'POST'])
def searchmultiple():
    #return request.values
    v= request.form.getlist('mytext[]')
    b= request.values["hidden_colonnes"].split(",")
    fields = getFields()
    tab=[]
    for i in range(len(v)):
        if b[i]=="number" or b[i]=="UID" :
            try:
                v[i] = int(v[i])
            except:
                return render_template('home/search.html', segment='searchmultiple',nbr=0)
        tab.append({b[i]:v[i]})
    data = getSearchMultiple(tab)
    personnes = data["hits"]["hits"]
    total = data["hits"]["total"]
    nbrPersonne = len(personnes)

    return render_template('home/search.html', segment='searchmultiple',total=total,fields=fields,personnes=personnes,nbr=nbrPersonne)

@blueprint.route('/<template>')
@login_required
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        # Serve the file (if exists) from app/templates/home/FILE.html
        return render_template("home/" + template, segment=segment)

    except TemplateNotFound:
        return render_template('home/page-404.html'), 404

    except:
        return render_template('home/page-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):

    try:

        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index'

        return segment

    except:
        return None
