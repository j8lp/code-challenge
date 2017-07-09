#Shutterfly Customer Lifetime Value Code Project

## Requirements

```bash
pip install python-dateutil flask_sqlalchemy currencyconverter sqlalchemy_utils
```


## How to run

```bash
python ./src/LifeTimeValueWS.py
```
This will start a Flask web server and initialize a new database if one is not already present.

To call the web service, go to http://localhost:5000/TopX/10

For more testing, see SoapUI project.


## Performance

Ingest takes O(D*n) time, where n is the number of events ingested and D is the time of a database look up or insert by primary key.

TopXSimpleLTVCustomers takes O(D*c*o*v*log(c)) time, where c, o, and v are the numbers of customers, orders, and visits respectively in the database.  