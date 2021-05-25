from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

################ TODO

# DB credentials from the Airflow connections

connection = BaseHook.get_connection("pg_local")

extras = connection.extra_dejson

db_host = connection.host
db_user = connection.login
db_password = connection.password

pg_url = f"jdbc:postgresql://{db_host}:5432/postgres"
pg_properties = {"user": f"{db_user}", "password": f"{db_password}"}

# Connect to Spark cluster
spark = SparkSession.builder\
            .config('spark.driver.extraClassPath' 
            , '/home/user/shared_folder/postgresql-42.2.20.jar')\
            .master('local')\
            .appName("test")\
            .getOrCreate()

# 1 - вывести количество фильмов в каждой категории, отсортировать по убыванию.

film_category_df = spark.read.jdbc(pg_url, "film_category", properties=pg_properties)
category_df      = spark.read.jdbc(pg_url, "category", properties=pg_properties)

film_category_df = film_category_df.join(category_df
                      , film_category_df.category_id == category_df.category_id
                      , 'left')\
                      .select(F.col('film_id'), F.col('name').alias('category_name'))

num_films_in_category = film_category_df.groupBy(F.col('category_name'))\
                            .agg(F.count(F.col('film_id')).alias('num_films'))\
                            .orderBy('num_films', ascending=False)


# 2 - вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

rental_df = spark.read.jdbc(pg_url, "rental", properties=pg_properties)
inventory_df = spark.read.jdbc(pg_url, "inventory", properties=pg_properties)
film_actor_df = spark.read.jdbc(pg_url, "film_actor", properties=pg_properties)
actor_df = spark.read.jdbc(pg_url, "actor", properties=pg_properties)

top_ten_actors_df = rental_df.join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id)\
                             .join(film_actor_df, inventory_df.film_id == film_actor_df.film_id)\
                             .join(actor_df, film_actor_df.actor_id == actor_df.actor_id)\
                             .select(F.concat(F.col('first_name'), F.lit(' '), F.col('last_name')).alias("actor_name"), F.col('rental_id'))

top_ten_actors = top_ten_actors_df.groupBy(F.col('actor_name'))\
                                            .agg(F.count(F.col('rental_id')).alias('num_rentals'))\
                                            .orderBy('num_rentals', ascending=False)\
                                            .limit(10)