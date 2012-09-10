
import os
import psycopg2

PG_BACKEND_SETTINGS = {
    "dbname" : "senzari_stats",
    "user" : "senzari_dev",
    "password" : "senzari_dev",
    "host" : "localhost"
}

def open_connection():
        connection = psycopg2.connect(database=PG_BACKEND_SETTINGS['dbname'],
                        host=PG_BACKEND_SETTINGS['host'],
                        user=PG_BACKEND_SETTINGS['user'],
                        password=PG_BACKEND_SETTINGS['password'])
        return connection

def is_metric_counter(metric):
    parts = metric.split('.', 1)
    return parts[0].endswith('_counts')

'''
Our condense phase consist in retrieving data from our latest_stats table,
which is supposed to be cleaned/consumed by this module hourly, and then
putting those stats in our daily_stats table, which records stats per day.

We keep record of the average, min and max value, as well as special
handling for counters (stats whose first part of the name ends with _counts,
as defined by carbon itself, such 'stats_count.something.else'), and for that
case we keep an extra column, which keeps track of the actual count.

Finally, we clean the latest_stats table.
'''
def condense():
    try:
        connection = open_connection()

        sql = """
        SELECT
         name,
         date_trunc('day', tstamp) as day,
         SUM(value)
        FROM latest_stats
         GROUP BY name, day
        """
        cursor = connection.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        for x in data:
            # sum_value is only used with counters.
            name = x[0]
            day = x[1]
            sum_value = x[2]
            is_counter = is_metric_counter(name)

            # TODO - Support everything else besides counters.
            if not is_counter:
                continue

            sql_daily = """
            SELECT value
            FROM daily_stats
            WHERE
             name=%s AND day=%s
            """
            cursor2 = connection.cursor()
            cursor2.execute(sql_daily, [name, day])
            daily_data = cursor2.fetchall()

            if len(daily_data) > 0:

                # Retrieve the our single row.
                for x in daily_data:
                    # FIXME: Do a correct average handling ;)
                    if (is_counter):
                        sum_value = sum_value + x[0]

                sql_daily_insert = """
                UPDATE daily_stats
                 SET value=%s
                 WHERE name=%s AND day=%s
                """
                cursor3 = connection.cursor()
                cursor.execute(sql_daily_insert, [sum_value,
                                                  name, day])
            else:
                sql_daily_insert = """
                INSERT
                 INTO daily_stats(name, day, value)
                 VALUES(%s, %s, %s)
                """
                cursor3 = connection.cursor()
                cursor.execute(sql_daily_insert, [name, day,
                                                  sum_value])

        clear_sql = """
            DELETE FROM latest_stats
        """
        clear_cursor = connection.cursor()
        clear_cursor.execute(clear_sql)

        connection.commit()
    except psycopg2.Warning, e:
        print "Warning while performing the sql operation: %s" % e
    except psycopg2.Error, e:
        print "Error while performing the sql operation: %s" % e

def main():
    condense()

if __name__ == "__main__":
    main()

