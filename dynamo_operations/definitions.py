from datetime import timedelta

import pytz


EXPIRY_DELTA = timedelta(days=3)
REGION_TIMEZONES = {
    'eu-west-1': pytz.timezone('Europe/Dublin')
}
