from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, MO, TU

def extract_target_location():
    last_monday = date.today() + relativedelta(weekday=MO(-1))
    s3_target_location = f"week_start={last_monday}"
    return s3_target_location

if __name__=="__main__":
    print(extract_target_location())
