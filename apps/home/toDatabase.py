from apps.home.models import ColonnesSearch, DocTypes
from apps import db

def create_doc_type(name,statut):
    
    doc_type = DocTypes(name, statut)
    db.session.add(doc_type)
    db.session.commit()

def get_doc_type(name):
    #deleteAll()
    try:
        
        return DocTypes.query.filter(DocTypes.name == name).first().name
        
    except:
        return False
def getAll_doc_type():
    
    try:
        
        return DocTypes.query.all()
    except:
        return []

def update_doc_type(name,statut):
    doc = DocTypes.query.filter_by(name = name).first()
    doc.statut = statut
    #doc.save()
    #db.session.add(doc)
    db.session.commit()

def deleteAll():
    doc = DocTypes.query.all()
    for d in doc:
        db.session.delete(d)
        db.session.commit()

### colonne search

def createColonneSearch(name):
    
    colonne = ColonnesSearch(name)
    db.session.add(colonne)
    db.session.commit()

def getColonneSearch(name):
    try:  
        return ColonnesSearch.query.filter(ColonnesSearch.name == name).first().name   
    except:
        return False
def getAllColonneSearch(): 
    try:
        
        return ColonnesSearch.query.all()
    except:
        return []
