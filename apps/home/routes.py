# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from apps.home import blueprint
from apps.home.toDatabase import createColonneSearch, getAllColonneSearch, getColonneSearch
from flask import render_template, request,flash,redirect,url_for
from flask_login import login_required
from jinja2 import TemplateNotFound
import os
from apps.home.thread import Compute
from apps.home.utils import mergeIndex,createOrUpdateDocType,getAllDocType, getData,getDataSearch,getFields,getSearchMultiple,process_csv
from datetime import datetime
import pandas as pd
import threading
import logging

log = logging.getLogger(__name__)
@blueprint.route('/index')
@login_required
def index():
    try:
        #mergeIndex("italia","italia1","number")
        data,colonnes = getData()
        fields = getAllColonneSearch()
        personnes = data["hits"]["hits"]
        total = data["hits"]["total"]["value"]
        nbrPersonne = len(personnes)
        nbrColonne = len(colonnes)
        return render_template('home/index.html', total=total,segment='index',nbrColonne=nbrColonne,fields=fields,colonnes=colonnes,personnes=personnes,nbr=nbrPersonne)
    except Exception as e:
        return str(e)



@blueprint.route('/imports')
def imports():
    doc_type = getAllDocType()
    return render_template('home/import.html', segment='imports',doc_type=doc_type)

ALLOWED_EXTENSIONS = set(['csv','txt'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@blueprint.route('/merge', methods=['GET', 'POST'])
def merge():
    doc_type = getAllDocType()
    if request.method == 'POST':
        index=request.values['index']
        commonField=request.values['commonField']
        startDate = datetime.now()
        createOrUpdateDocType(index,"pending....")

        try:
            mergeIndex(index,commonField)
        except Exception as e:
            flash('Erreur jointure %s' %str(e),'danger')
            return render_template('home/merge_index.html', segment='merge',doc_type=doc_type)

        endDate = datetime.now() - startDate
        tmin = round((endDate.total_seconds())/60,4)
        createOrUpdateDocType(index,"terminé en %s minute"%tmin)
        
        
        flash('Start jointure','success')
        return render_template('home/merge_index.html', segment='merge',doc_type=doc_type)
    return render_template('home/merge_index.html', segment='merge',doc_type=doc_type)


@blueprint.route('/upload', methods=['GET', 'POST'])
def upload_file():
    fields = getAllColonneSearch()
    doc_type = getAllDocType()
    if request.method == 'POST':
        file = request.files['file']
        delimiter=request.values['delimiter']
        index_type=request.values['index_type']
        if index_type=="Other" and request.values['new_index_type'] != "":
            index_type=request.values['new_index_type']

        if file and allowed_file(file.filename):
        #return f'uploaded {f.filename}'
            rep ="/home/abdoulayesarr/Documents/Digital_management/tmp"
            #rep ="/home/data/Documents/dm/tmp"
            new_filename = f'{index_type}_{file.filename.split(".")[0]}_{str(datetime.now())}.csv'
            file.save(os.path.join(rep,new_filename))
            #output = process_csv(os.path.join(rep,new_filename))
            filename=os.path.join(rep,new_filename)
            #with open(os.path.join(rep,new_filename)) as file:
            if(delimiter=="tab"):
                delimiter="\t"
            data = pd.read_csv(filename,header=0,sep=delimiter,encoding= 'unicode_escape') 
            colonnes=[]
            
            myData = data.values[:5]
            for row in data.columns:
                colonnes.append(row)
          
            nbrColonne = len(colonnes)
            return render_template("home/s3_csv_table.html",segment='imports',filename=filename,delimiter=delimiter, fields=fields,nbrColonne=nbrColonne,myData=myData,colonnes=colonnes)
            
        else:
            flash('format de fichier incorrect','danger')
            #return redirect(url_for('home_blueprint.upload_file'))
            return render_template('home/import.html',doc_type=doc_type)

@blueprint.route('/saved_file', methods=['GET', 'POST'])
def saved_file():
    doc_type = getAllDocType()
    if request.method == 'POST':
        file = request.values['filename']
        sep = request.values['sep']
        header = request.values['header']
        colonnes= request.values["hidden_colonnes"].split(",")
        cols= request.form.getlist('mytext[]')
        colsHidden= request.form.getlist('colsHidden[]')
        doc_type=(file.split("/")[-1]).split("_")[0]
        #if(header=="oui"):
        try:
            
            startDate = datetime.now()
            createOrUpdateDocType(doc_type,"pending....")
            process_csv(file,sep,colonnes,header,cols,colsHidden)
            endDate = datetime.now() - startDate
            tmin = round((endDate.total_seconds())/60,4)
            createOrUpdateDocType(doc_type,"terminé en %s minute"%tmin)
            # thread_a = Compute(doc_type,file,sep,colonnes,header,cols)
            # thread_a.start()
            flash('file loqd successfull','success')
            return render_template('home/import.html',segment='imports',doc_type=doc_type)
        except:
            flash('Erreur, file error','danger')
            return render_template('home/import.html',segment='imports',doc_type=doc_type)

    return request.values
# def background(doc_type,file,sep,colonnes,header,cols):
#     startDate = datetime.now()
#     createOrUpdateDocType(doc_type,"pending....")
#     process_csv(file,sep,colonnes,header,cols)
#     endDate = datetime.now() - startDate
#     tmin = round((endDate.total_seconds())/60,4)
#     createOrUpdateDocType(doc_type,"terminé en %s minute"%tmin)
@blueprint.route('/search',methods=['GET', 'POST'])
def search():
    try:
        colonnes,value = request.values["hidden_colonnes"].split(","),request.values["value"]
        if request.values["value"]=='':
            return redirect(url_for('home_blueprint.index'))
        
        #fields = getAllColonneSearch()
        fields = getFields()
        data,colonnes = getDataSearch(colonnes,value)
        total = data["hits"]["total"]["value"]
        personnes = data["hits"]["hits"]
        nbrPersonne = len(personnes)
        
        return render_template('home/index.html',total=total, segment='index',colonnes=colonnes,fields=fields,personnes=personnes,nbr=nbrPersonne)
    except Exception as e:
        return "error %s" % (str(e))

@blueprint.route('/search_by_colonne')
def searchByColonne():
    fields = getAllColonneSearch()
    return render_template('home/search.html', segment='searchmultiple',nbr=0)
    # except Exception as e:
    #     return str(e)

@blueprint.route('/searchmultiple',methods=['GET', 'POST'])
def searchmultiple():
    #return request.values
    v= request.form.getlist('mytext[]')
    b= request.values["hidden_colonnes"].split(",")
    fields = getAllColonneSearch()
    tab=[]
    for i in range(len(v)):
        if b[i]=="number" or b[i]=="UID" :
            try:
                v[i] = int(v[i])
            except:
                return render_template('home/search.html', segment='searchmultiple',nbr=0)
        tab.append({b[i]:v[i]})
    data,colonnes = getSearchMultiple(tab)
    personnes = data["hits"]["hits"]
    total = data["hits"]["total"]["value"]
    nbrPersonne = len(personnes)

    return render_template('home/search.html', segment='searchmultiple',colonnes=colonnes,total=total,fields=fields,personnes=personnes,nbr=nbrPersonne)

@blueprint.route('/statut')
def statut():
    doc_type = getAllDocType()
    return render_template('home/statut.html', segment='statut',doc_type=doc_type)

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

@blueprint.route('/create_colonne', methods=['GET', 'POST'])
def create_colonne():
    log.debug("all colonnes %s"% getAllColonneSearch())
    if request.method == 'POST':
        name = request.values['name']
        if name== getColonneSearch(name):
            flash("colonne existe déjà ","danger")
            return redirect(url_for('home_blueprint.create_colonne'))
        else:
            try:
                createColonneSearch(name)
                flash("colonne créer avec succés ","success")
                return redirect(url_for('home_blueprint.create_colonne'))
            except Exception as e:
                log.error("erreur création colonne %s" % str(e))
                flash("Erreur interne","danger")
                return redirect(url_for('home_blueprint.create_colonne'))
    
    return  render_template('home/create_colonne.html', segment='colonne_search')
