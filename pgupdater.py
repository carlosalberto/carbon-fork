
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
         SUM(value),
         AVG(value) as average,
         MIN(value) as min,
         MAX(value) as max
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
            average = x[3]
            min_value = x[4]
            max_value = x[5]
            is_counter = is_metric_counter(name)

            sql_daily = """
            SELECT
             average,
             min,
             max,
             counter
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
                    average = (x[0] + average) / 2.0
                    min_value = min(min_value, x[1])
                    max_value = max(max_value, x[2])
                    if (is_counter):
                        sum_value = sum_value + x[3]

                sql_daily_insert = """
                UPDATE daily_stats
                 SET average=%s, min=%s, max=%s, counter=%s
                 WHERE name=%s AND day=%s
                """
                cursor3 = connection.cursor()
                cursor.execute(sql_daily_insert, [average, min_value, max_value,
                                                  sum_value,
                                                  name, day])
            else:
                sql_daily_insert = """
                INSERT
                 INTO daily_stats(name, day, average, min, max, counter)
                 VALUES(%s, %s, %s, %s, %s, %s)
                """
                cursor3 = connection.cursor()
                cursor.execute(sql_daily_insert, [name, day, average,
                                                  min_value, max_value,
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

