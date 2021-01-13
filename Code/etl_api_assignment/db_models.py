from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Numeric

from etl_api_assignment.config import db_details

user_name = db_details['user_name']
password = db_details['password']
server = db_details['server']
backend = db_details['backend']

MSSQL_URL = f"""mssql+pyodbc://{user_name}:{password}@{server}/{backend}"""

engine = create_engine(MSSQL_URL + "?driver=SQL+Server+Native+Client+11.0")
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Model = declarative_base(name='Model')
Model.query = db_session.query_property()
metadata = MetaData(Model.metadata)


class Tip(Model):
    __tablename__ = 'tip'

    pid = Column(Integer, primary_key=True, autoincrement=True)
    tipAmount = Column(Numeric(12, 2))
    quarter = Column(Integer)
    year = Column(Integer)

    @staticmethod
    def get_tip_amount_based_on_year_and_quarter(year, quarter_id):
        """Queries `tip` table to fetch tipAmount based on given year and quarter"""
        tip_amount = db_session.query(Tip.tipAmount).filter_by(year=year, quarter=quarter_id).first()
        if not tip_amount:
            results = {"maxTipPercentage": "Not available for the given year and quarter"}
        else:
            results = {"maxTipPercentage": float(tip_amount[0])}
        return results

    @staticmethod
    def get_tip_amount_based_on_year(year):
        """Queries `tip` table to fetch tipAmount by aggregating all the quarters for a particular year"""
        total_tip_amount_of_all_quarters = db_session.query(Tip.tipAmount, Tip.quarter).filter_by(
            year=year).all()
        if not total_tip_amount_of_all_quarters:
            results = {"maxTipPercentage": "Not available for the given year"}
        else:
            res = []
            for total_tip_perc, quarter_id in total_tip_amount_of_all_quarters:
                res.append({"quarter": quarter_id, "maxTipPercentage": float(total_tip_perc)})
            results = {"maxTipPercentages": res}

        return results


class TripSpeed(Model):
    __tablename__ = 'tripSpeed'
    pid = Column(Integer, primary_key=True, autoincrement=True)
    calendarHour = Column(Integer)
    calendarDay = Column(Integer)
    calendarMonth = Column(Integer)
    calendarYear = Column(Integer)
    maxSpeed = Column(Numeric(12, 2))

    @staticmethod
    def get_max_speed_by_year_month_day(year, calendar_month=None, calendar_day=None):
        """Queries `tripSpeed` table to fetch calendarHour,maxSpeed for given calendar year, month and day"""
        max_speed_by_hour_basis = db_session.query(TripSpeed.calendarHour, TripSpeed.maxSpeed).filter_by(
            calendarYear=year, calendarMonth=calendar_month, calendarDay=calendar_day).all()
        results = []

        if not max_speed_by_hour_basis:
            results = {"tripSpeeds": "Not available for the given year"}
        else:
            res = []
            for hour, max_speed in max_speed_by_hour_basis:
                res.append({"Hour": hour, "maxSpeed": f"{max_speed}mph"})
                results = {"tripSpeeds": res}

        return results

# Model.metadata.create_all(engine)
