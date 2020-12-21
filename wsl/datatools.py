import csv
import pyodbc


class DataTools:
    """A set of static methods for save data"""

    @staticmethod
    def save_to_csv(data, filepathname, headers, mode='w', delimiter=','):
        """Save a set of records to a csv file"""
        if mode != 'w':
            with open(filepathname, mode=mode, newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=delimiter)
                writer.writerows(data)
        else:
            with open(filepathname, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=delimiter)
                writer.writerow(headers)
                writer.writerows(data)

    @staticmethod
    def save_to_database(data, connection_str, insert_query, **kwargs):
        """Save a set of records to a database"""
        with pyodbc.connect(connection_str, **kwargs) as conn:
            cursor = conn.cursor()
            for row in data:
                try:
                    cursor.execute(insert_query, row)
                except pyodbc.IntegrityError:  # duplicate records
                    continue


class ConnectionString:
    """A container class for building connection strings"""
    default = {
        'driver': 'ODBC DRIVER 17 FOR SQL SERVER',
        'server': r'.',
        'database': 'MERCURY',
        'trusted_connection': 'YES',
        'uid': '',
        'pwd': ''
    }

    @staticmethod
    def build(
            driver=default['driver'],
            server=default['server'],
            database=default['database'],
            trusted_connection=default['trusted_connection'],
            uid=default['uid'],
            pwd=default['pwd']):

        if uid and pwd:
            return f"DRIVER={{{driver}}}; SERVER={server}; DATABASE={database}; UID={uid}; PWD={pwd};"
        else:
            return f"DRIVER={{{driver}}}; SERVER={server}; DATABASE={database}; TRUSTED_CONNECTION={trusted_connection};"
