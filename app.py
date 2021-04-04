import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect
Base = automap_base()

# Save reference to the table
Base.prepare(engine, reflect=True)

Measurement=Base.classes.measurement
Station=Base.classes.station
#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"<b>Aloha !!!!</b> This is the Hawaii Weather Database.<br/><br/>"
        f"Here, You will find the following routes:<br/><br/>"
        f"/api/v1.0/lastmeasurement<br/>"
        f"This API route obtains the last measurement in the database<br/><br/>"
        f"/api/v1.0/precipitation<br/>"
        f"This API route obtains the last 12 months of precipitation measurements in the database<br/><br/>"
        f"/api/v1.0/stations<br/>"
        f"This API route obtains the list of stations in the database<br/><br/>"
        f"/api/v1.0/tobslastyear<br/>"
        f"This API route obtains the last 12 months of temperature measurements in the most active station<br/><br/>"
        f"/api/v1.0/tobspreviousy<br/>"
        f"This API route obtains the previous to last year of precipitation measurements in the database<br/><br/>"
        f"/api/v1.0/YYYY-MM-DD<br/>"
        f"This API route obtains the min T, the average T, and the max T for a period of time starting with given date in the format YYYY-MM-DD.<br/><br/>"
        f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD<br/>"
        f"This API route obtains the min T, the average T, and the max T for the period of time between the two given dates in the format YYYY-MM-DD.<br/><br/>"
    )

@app.route("/api/v1.0/lastmeasurement")
def lastmeasurement():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Query last measurement in the database
    last_measurement=session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    session.close()

    return jsonify(last_measurement)

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    session = Session(engine)
    
    # Query last 12 months of measurement in the database for precipitation
    
    query_12_months_r = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date.between('2016-08-23', '2017-08-23'))
    session.close()
    rain_list=[]
    for date, prcp in query_12_months_r:
        rain_dict={}
        rain_dict["date"] = date
        rain_dict["prcp"] = prcp
        rain_list.append(rain_dict)
   
    return jsonify(rain_list)
 

@app.route("/api/v1.0/stations")
def stations():
    
    session = Session(engine)
    
    # Query list of stations
    station_list=session.query(Station.station).all()

    session.close()

    return jsonify(station_list)   

@app.route("/api/v1.0/tobslastyear")
def tobslastyear():
    session = Session(engine)
    
    # Query by grouping and counting the most active station
    count_per_station=session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).all()
    count_per_station_df=pd.DataFrame.from_records(count_per_station, columns=['station', 'records'])

    #Obtaining the first value by sorting and using "iloc"
    count_per_station_df=count_per_station_df.sort_values(by=['records'], ascending=False)
    most_act=count_per_station_df['station'].iloc[0]
    
    #Query temperature values by the most active station and for the last 12 monts  
    fecha=[]
    t=[]
    estacion=[]
    temp_y=session.query(Measurement.date,Measurement.tobs,Measurement.station).filter_by(station=most_act).filter(Measurement.date.between('2016-08-23', '2017-08-23'))

    session.close()

    #Obtaining values for date, temperature and station, then, converting to a single list to obtain a jason response
    for x in temp_y:
        fecha.append(x.date)
        t.append(x.tobs)
        estacion.append(x.station)
        
    fecha_df=pd.DataFrame(fecha)
    fecha_df['Date']=pd.DataFrame(fecha_df)

    t_df=pd.DataFrame(t)
    t_df['Temp']=pd.DataFrame(t_df)

    t_1y_df= pd.concat([fecha_df,t_df], axis=1)
    del t_1y_df[0]  
    t_1y=t_1y_df.values.tolist()
    
    return jsonify(t_1y)



@app.route("/api/v1.0/tobspreviousy")
def tobspreviousy():
    session = Session(engine)
          
    temp1=[]
    
    #Query for temperature measurements of the previous to last year
    query_12_months_t = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date.between('2015-08-23', '2016-08-23'))
    session.close()

    for y in query_12_months_t:
        temp1.append(y.tobs)
    
    return jsonify(temp1)


@app.route("/api/v1.0/<start>")
def temp_summary_starting(start):
    session = Session(engine) 
    
    #Query for temperature measurements: Min, Max and Average from a start date
    q_t_start= session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs),func.max(Measurement.tobs)).filter(Measurement.date>=start).all()
    session.close()
    
    #Accessing values of the list/tuple to later create a dictionary
    mint=q_t_start[0][0]
    avgt=q_t_start[0][1]
    maxt=q_t_start[0][2]
    
    values_dic={'Min_temp=':mint,'Avg_temp=':avgt,'Max_temp=':maxt}
    
    return jsonify(values_dic)



@app.route("/api/v1.0/<start>/<end>")
def temp_summary_range(start,end):
    session = Session(engine) 
    
    #Query for temperature measurements: Min, Max and Average from a range date
    q_t_range= session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs),func.max(Measurement.tobs)).filter(Measurement.date>=start).filter(Measurement.date<=end).all()
    
    session.close()
    
    #Accessing values of the list/tuple to later create a dictionary
    mint1=q_t_range[0][0]
    avgt1=q_t_range[0][1]
    maxt1=q_t_range[0][2]
    
    values_dic1={'Min_temp=':mint1,'Avg_temp=':avgt1,'Max_temp=':maxt1}
    
    return jsonify(values_dic1)



if __name__ == '__main__':
    app.run(debug=True)