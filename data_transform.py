import logging


class DataCalculationTask():
    def __init__(self):
        self.rainless_conditions: set = {
            'clear',
            'partly-cloudy',
            'cloudy',
            'overcast',
            'drizzle'
        }

    def _get_daily_statistics_by_hours(self, hours: dict) -> tuple[int, float, int]:
        rainless_hours: int = 0
        sum_temp_per_day: float = 0.0
        num_of_hours: int = 0
        for hour in hours:
            if 9 <= int(hour['hour']) <= 19:
                num_of_hours += 1
                sum_temp_per_day += hour['temp']
                if hour['condition'] in self.rainless_conditions:
                    rainless_hours += 1
        return rainless_hours, sum_temp_per_day, num_of_hours

    def get_data_calculation(self, city: dict) -> list:
        result: dict = {}
        city_name = city['geo_object']['locality']['name']
        logging.info(f'Start calculating city data: {city_name}')
        try:
            result['city'] = city_name
            result['days'] = []
            num_of_days: int = 0
            sum_avg_temps_for_all_days: float = 0.0
            sum_rainless_hours_for_all_days: int = 0
            for forecast in city['forecasts']:
                result['days'].append({'date': forecast['date']})
                rainless_hours, sum_temp_per_day, num_of_hours = self._get_daily_statistics_by_hours(forecast['hours'])
                if num_of_hours == 11:
                    num_of_days += 1
                    result['days'][-1]['rainless hours'] = rainless_hours
                    avg_temp_per_day: float = round(sum_temp_per_day / 11, 1)
                    sum_avg_temps_for_all_days += avg_temp_per_day
                    sum_rainless_hours_for_all_days += rainless_hours
                    result['days'][-1]['averange temperature per day'] = avg_temp_per_day
            result['average temp for all days'] = round(sum_avg_temps_for_all_days / num_of_days, 1)
            result['average rainless hours for all days'] = round(sum_rainless_hours_for_all_days / num_of_days, 1)
        except Exception as error:
            logging.exception(f'Data calculation error: {error}')
            raise error
        logging.info(f'End of city data calculation: {city_name}')
        return result
