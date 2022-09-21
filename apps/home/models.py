# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""


from apps import db


class DocTypes(db.Model):

    __tablename__ = 'DocTypes'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(164))
    statut = db.Column(db.String(164))
    
    def __init__(self, name,statut):
        self.name = name
        self.statut = statut



    def __repr__(self):
        return str(self.name)


