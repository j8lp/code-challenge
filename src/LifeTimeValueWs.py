from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
import json
import dateutil.parser
import dateutil.rrule

from currency_converter import CurrencyConverter
from sqlalchemy_utils import aggregated
from sqlalchemy.ext.hybrid import hybrid_property,hybrid_method

app = Flask(__name__)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///D.db'
D = SQLAlchemy(app)
CC = CurrencyConverter()

class CUSTOMER(D.Model):
    key = D.Column(D.String, primary_key=True)
    event_time = D.Column(D.DateTime)
    last_name = D.Column(D.String)
    adr_city = D.Column(D.String)
    adr_state = D.Column(D.String)
    orders = D.relationship('ORDER')
    site_visits = D.relationship('SITE_VISIT')

    @aggregated('orders', D.Column(D.DateTime))
    def earliest_order(self):
        return func.min(ORDER.event_time)
    @aggregated('site_visits', D.Column(D.DateTime))
    def earliest_visit(self):
        return func.min(SITE_VISIT.event_time)

    # Total order revenue divided by total number of visits
    @hybrid_property
    def  revenueOverVisits(self):
        t =  sum([order.total_amount for order in self.orders]) 
        try:
            return t / len(self.site_visits)  
        except ZeroDivisionError:
            return t

    # Number of visits divided by total number of weeks since customer's earliest visit or order.  Takes latest time as input
    @hybrid_method
    def  visitsOverWeeks(self,latest):
        # If customer has no visits or orders, then likely customer just joined.  
        # So record latest time as customer's first visit
        earliest = [latest,self.earliest_visit,self.earliest_order]
        visits = len(self.site_visits)  
        earliest = min([d for d in earliest if d is not None])
        weeks = dateutil.rrule.rrule(dateutil.rrule.WEEKLY, dtstart=earliest, until=latest).count()
        return visits / weeks



class SITE_VISIT(D.Model):
    key = D.Column(D.String, primary_key=True)
    event_time = D.Column(D.DateTime,primary_key=True)
    customer_id = D.Column(D.String, D.ForeignKey('CUSTOMER.key'),
       primary_key=True)
    tags = D.Column(D.String)

class IMAGE(D.Model):
    key = D.Column(D.String, primary_key=True)
    event_time = D.Column(D.DateTime,primary_key=True)
    customer_id = D.Column(D.String, D.ForeignKey('CUSTOMER.key'),
       primary_key=True)
    camera_make = D.Column(D.String)
    camera_model = D.Column(D.String)

class ORDER(D.Model):
    key = D.Column(D.String, primary_key=True)
    event_time = D.Column(D.DateTime,nullable=False)
    customer_id = D.Column(D.String, D.ForeignKey('CUSTOMER.key'),
       nullable=False)
    total_amount = D.Column(D.Float,nullable=False)

D.create_all()


# Expose through web services
@app.route("/Events/Ingest",methods=['POST'])
def Ingest_POST():
    e = request.json
    Ingest(e,D)
    return jsonify("Success")

def Ingest(e,D):
    # If e is array of events, then parse each one individually
    if isinstance(e,list):
        for x in e:
            Ingest(x,D)
    else:
        eventType = eval(e['type'])
        e['event_time'] =  dateutil.parser.parse(e['event_time'])

        #Convert total_amount to decimal using currency converter plugin
        if "total_amount" in e:
            e["total_amount"] = CC.convert(*str(e["total_amount"]).split(),"USD")
        #Convert tags array to string.  
        if 'tags' in e:
            e['tags'] = str(e['tags'])
        #Don't need to store type and verb entries
        del e['type']
        del e['verb']
        event = eventType(**e)
        D.session.merge(event)
    D.session.commit()




@app.route('/Customers/Top/<x>')
def TopX_GET(x):
   return jsonify(TopXSimpleLTVCustomers(int(x),D))

def TopXSimpleLTVCustomers(x, D):

    latest_order = D.session.query(func.max(ORDER.event_time)).one()[0]
    latest_visit = D.session.query(func.max(SITE_VISIT.event_time)).one()[0]
    latest = [latest_visit,latest_order]
    latest = max([d for d in latest if d is not None])
    total_amount = D.session.query(func.sum(ORDER.total_amount)).group_by(ORDER.customer_id).subquery()
    qry = D.session.query(CUSTOMER)
    LTVs = []
    for _res in qry.all():
        # Compute LTV 
        # 52(a) x t. Where a is the average customer value per week (customer expenditures per visit (USD) x number of site visits per week) and t is the average customer lifespan. The average lifespan for Shutterfly is 10 years
        LTV = 52 * _res.revenueOverVisits * _res.visitsOverWeeks(latest)
        # Round to two decimal places for clarity
        LTVs.append((_res.key,_res.last_name,round(LTV,2)))
    LTVs = sorted(LTVs, reverse=True, key=lambda x: x[2])
    return  LTVs[:x]

if __name__ == '__main__':
    app.run(debug=False)