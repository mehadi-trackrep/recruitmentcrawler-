import re
import json
import hashlib
import datetime
from dateutil import parser


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)


class Json:
    @staticmethod
    def dumps(data_dict, indent=None):
        return json.dumps(data_dict, cls=JsonEncoder, indent=indent, ensure_ascii=False)


def convert_to_datetime(date_string):
    return datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')


def get_utc_time():
    date_str = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    return convert_to_datetime(date_string=date_str)


def fix_time_format(string_date):
    try:
        date_obj = None
        if isinstance(string_date, int):
            date_obj = datetime.datetime.fromtimestamp(string_date / 1e3)
        if isinstance(string_date, str):
            date_obj = parser.parse(string_date)
        return date_obj.strftime('%Y-%m-%dT%H:%M:%S')
    except Exception:
        return get_utc_time()


def string_to_md5(text):
    m = hashlib.md5()
    m.update(text.encode('utf-8'))
    return m.hexdigest()

def to_json(data):
    return json.dumps(data, ensure_ascii=False, default=str)

def get_required_recruiment_date():
    return datetime.date.today()-datetime.timedelta(1)

def check_none_type_var(message):
    if not message:
        message = ""

    return message
 
def clean_text(data):
    if not isinstance(data, str):
        return ""

    input_string = data.replace('\r', '')

    input_string = input_string.strip()
    input_string = re.sub(r"[\n]+", "\n", input_string)

    regex = re.compile(r'\.{2,}')
    input_string = regex.sub('', input_string)  # Remove Multi Dots (More Than 1)

    regex = re.compile(r'-{2,}')
    input_string = regex.sub('', input_string)  # Remove Multi Minus (More Than 1)

    regex = re.compile(r'\+{2,}')
    input_string = regex.sub('', input_string)  # Remove Multi Plus (More Than 1)

    regex = re.compile(r'•{1,}')
    input_string = regex.sub('', input_string)  # Remove Unicode ·

    regex = re.compile(r'·{1,}')
    input_string = regex.sub('', input_string)  # Remove Unicode

    regex = re.compile(r'►{1,}')
    input_string = regex.sub('', input_string)  # Remove Unicode ►

    consecutive_ques = re.compile(r'\?{2,}')
    input_string = consecutive_ques.sub('?', input_string)  # Remove Multi Question Mark

    regex = re.compile(r'!{2,}')
    input_string = regex.sub('', input_string)  # Remove Multi Exclamatory (More Than 1)

    consecutive_space = re.compile(r'\*{1,}')
    input_string = consecutive_space.sub('', input_string)  # Remove Star

    consecutive_space = re.compile(r' {2,}')
    input_string = consecutive_space.sub(' ', input_string)  # Remove Multi Space

    input_string = input_string.strip()

    return input_string