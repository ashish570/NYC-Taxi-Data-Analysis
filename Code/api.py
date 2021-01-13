from flask import Flask, render_template, request, jsonify

from etl_api_assignment.db_models import Tip, TripSpeed

app = Flask(__name__)


@app.route('/api/tip/<int:year>/<int:quarter_id>/max', methods=['GET'])
def get_tip_amount_year_quarter(year=None, quarter_id=None):
    """API to fetch `maxTipPercentage` by `year` and `quarter`"""
    error_message = {"error_message": ""}
    error_flag = False
    if request.method == 'GET':

        if not year or not quarter_id:
            error_message[
                'error_message'] = 'year or quarter_number need to be passed in url as `/api/tip/<year>/<quarter_id>/max`'
            error_flag = True

        if error_flag:
            return jsonify(error_message)

        results = Tip.get_tip_amount_based_on_year_and_quarter(year=year, quarter_id=quarter_id)

        return jsonify(results)


@app.route('/api/tips/<int:year>/max', methods=['GET'])
def get_tip_amount_by_year(year=None):
    """API to fetch `maxTipPercentages` by `year`"""
    error_message = {"error_message": ""}
    error_flag = False
    if request.method == 'GET':

        if not year:
            error_message['error_message'] = 'year need to be passed in url as `/api/tip/<year>/max`'
            error_flag = True

        if error_flag:
            return jsonify(error_message)

        results = Tip.get_tip_amount_based_on_year(year=year)

        return jsonify(results)


@app.route('/api/speed/<int:year>/<int:calendar_month>/<int:calendar_day>/max', methods=['GET'])
def get_speed_by_day_month_year(year=None, calendar_month=None, calendar_day=None):
    """API to fetch `maxTipPercentages` by `year`"""
    error_message = {"error_message": ""}
    error_flag = False
    if request.method == 'GET':

        if not year or not calendar_month or not calendar_day:
            error_message[
                'error_message'] = """`year` or `calendar_month` or `calendar_day` need to be passed in url as 
                `/api/speed/<int:year>/<int:calendar_month>/<int:calendar_day>/max`"""
            error_flag = True

        if error_flag:
            return jsonify(error_message)

        results = TripSpeed.get_max_speed_by_year_month_day(year=year, calendar_day=calendar_day,
                                                            calendar_month=calendar_month)

        return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0') # remove debug for production scenario
