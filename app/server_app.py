from fastapi import FastAPI
import psycopg2

connection = psycopg2.connect(user='airflow', password='airflow', host='host.docker.internal', port='5432', database='postgres')

# sql query to list of [category] ordered by their rating score, which we will calculate by the
# average of movie ratings, with Name,Score, Number of Titles as Principal, total number of runtime minutes

get_list_act = "select names.name as _name, AVG(av_rating) as score, Count(names.name) as ntitles, SUM(runtime_minutes) " \
             "as truntime from names inner join titles_names on name_id = names.id join titles on title_id = " \
             "titles.id where category = {} group by _name, category; "
app = FastAPI()


@app.get("/api/{category}")
def get_list_db(category: str):
    category = "'" + category + "'"
    with connection:
        with connection.cursor() as cursor:
            comand = get_list_act.format(category)
            cursor.execute(comand)
            average = cursor.fetchall()
            print(average)
    return {"list": average}

