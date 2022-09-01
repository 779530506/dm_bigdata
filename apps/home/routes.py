# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from apps.home import blueprint
from flask import render_template, request,flash,redirect,url_for
from flask_login import login_required
from jinja2 import TemplateNotFound
import os
from apps.home.utils import getData,getDataSearch,getFields,getSearchMultiple


@blueprint.route('/index')
@login_required
def index():
    try:
        data = getData()
        fields = getFields()
        personnes = data["hits"]["hits"]
        nbrPersonne = len(personnes)
        return render_template('home/index.html', segment='index',fields=fields,personnes=personnes,nbr=nbrPersonne)
    except Exception as e:
        return str(e)

@blueprint.route('/imports')
def imports():
    return render_template('home/import.html', segment='imports')

@blueprint.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        
        #return f'uploaded {f.filename}'
        rep ="/home/abdoulayesarr/Documents/Digital_management/tmp"
        #rep ="/home/data/Documents/dm/tmp"
        f.save(os.path.join(rep,'file1_index_;'))
        flash('file loaded successful','success')
        return render_template('home/import.html')



@blueprint.route('/search',methods=['GET', 'POST'])
def search():
    try:
        colonnes,value = request.values["hidden_colonnes"].split(","),request.values["value"]
        if request.values["value"]=='':
            return redirect(url_for('home_blueprint.index'))
        
        fields = getFields()
        data = getDataSearch(colonnes,value)
        personnes = data["hits"]["hits"]
        nbrPersonne = len(personnes)
        return render_template('home/index.html', segment='index',fields=fields,personnes=personnes,nbr=nbrPersonne)
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
    return render_template('home/search.html', segment='search',nbr=0)
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
        tab.append({b[i]:v[i]})
    data = getSearchMultiple(tab)
    personnes = data["hits"]["hits"]
    nbrPersonne = len(personnes)

    return render_template('home/search.html', segment='index',fields=fields,personnes=personnes,nbr=nbrPersonne)

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
