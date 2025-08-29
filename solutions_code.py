# Database connection settings (match environment values in docker-compose.yml)
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from psycopg2 import OperationalError, Error
from time import time


# Challenge: Use the proper port and use try-except-finally block(s)
def create_connection():
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="bigdatatools1",
            user="psqluser",
            password="psqlpass"
        )
        print("Database connected successfully")
        
    except ConnectionError as e:
        print(f"Connection Error:\n{e}")
        conn = None
    finally:
        return conn
    
conn = create_connection()

if conn is not None:
    try:
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("PostgreSQL version:", version[0])
    except Error as e:
        print(f"Error in query exec:\n{e}")
    finally:
        cur.close()
        conn.close()
        
# Challenge: perform the same operation using SQL inserts and Pandas, without optimizations


table_name = 'country_indicators_insert'

# Reconnect to the database
conn = create_connection()
cur = conn.cursor()

# create new table, same structure
cur.execute(f"""
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    country_name TEXT,
    country_code VARCHAR(3),
    indicator_name TEXT,
    indicator_code TEXT,
    year INTEGER,
    value NUMERIC
);
""")
conn.commit()
print(f"Table '{table_name}' created successfully.")

# Insert query for each row (!!!)
insert_query = f"""
INSERT INTO {table_name}
(country_name, country_code, indicator_name, indicator_code, year, value)
VALUES (%s, %s, %s, %s, %s, %s);
"""

row_count = 0
init = time()

for _, row in combined_data.iterrows():
    cur.execute(insert_query, (
        row['country_name'], row['country_code'],
        row['indicator_name'], row['indicator_code'],
        int(row['year']), row['value']
    ))
    row_count += 1

end = time()
time_insert = (end - init)/1000
conn.commit()
print(f"Insertion completed successfully. Total rows inserted: {row_count}")

# Verify rows inserted
cur.execute(f"SELECT COUNT(*) FROM {table_name};")
print(f"Verification: {cur.fetchone()[0]} rows in table '{table_name}'.")
print(f"Time spent: {time_insert:.2f}ms")


cur.close()
conn.close()

print("Comparison COPY vs INSERT method")
print(f"Time COPY: {time_copy:.2f}") #time_copy defined in copy method above
print(f"Time INSERT: {time_insert:.2f}")
print(f"Copy method is {(time_copy/time_insert)*100}% more efficient")


# Challenge: Include ASEAN and European Union in the Analysis


db_url = "postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1"
engine = create_engine(db_url)

query2 = """
        WITH brics_country_gdp AS (
            -- Step 1: Calculate total GDP for each individual BRICS country using a JOIN
            SELECT ci.year,
                   ci.country_name,
                   (MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' THEN ci.value END) *
                    MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END)) AS total_gdp
            FROM country_indicators ci
                     JOIN country_classification cc ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'BRICS'
              AND ci.indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
            GROUP BY ci.year, ci.country_name
            -- Ensure both population and gdp_per_capita exist for the calculation
            HAVING MAX(CASE
                           WHEN ci.indicator_name = 'GDP per capita PPP (current international $)'
                               THEN ci.value END) IS NOT NULL
               AND MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END) IS NOT NULL),
            brics_total_gdp AS (
                 -- Step 1A: Sum the GDP of all BRICS countries for each year
                 SELECT
                     year, 'BRICS (Calculated)' AS entity, SUM (total_gdp) AS total_gdp_ppp
                 FROM brics_country_gdp
                 GROUP BY year
            ),

            asean_country_gdp AS (
            -- Step 2: Calculate total GDP for each individual ASEAN country using a JOIN
            SELECT ci.year,
                   ci.country_name,
                   (MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' THEN ci.value END) *
                    MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END)) AS total_gdp
            FROM country_indicators ci
                     JOIN country_classification cc ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'ASEAN'
              AND ci.indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
            GROUP BY ci.year, ci.country_name
            HAVING MAX(CASE
                           WHEN ci.indicator_name = 'GDP per capita PPP (current international $)'
                               THEN ci.value END) IS NOT NULL
               AND MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END) IS NOT NULL),
            asean_total_gdp AS (
                 -- Step 2A: Sum the GDP of all ASEAN countries for each year
                 SELECT
                     year, 'ASEAN (Calculated)' AS entity, SUM (total_gdp) AS total_gdp_ppp
                 FROM asean_country_gdp
                 GROUP BY year
            ),

            eu_country_gdp AS (
            -- Step 3: Calculate total GDP for each individual European Union country (EU or EURO_ZONE) using a JOIN
            SELECT ci.year,
                   ci.country_name,
                   (MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' THEN ci.value END) *
                    MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END)) AS total_gdp
            FROM country_indicators ci
                     JOIN country_classification cc ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc IN ('EU','EURO_ZONE')
              AND ci.indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
            GROUP BY ci.year, ci.country_name
            HAVING MAX(CASE
                           WHEN ci.indicator_name = 'GDP per capita PPP (current international $)'
                               THEN ci.value END) IS NOT NULL
               AND MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END) IS NOT NULL),
            eu_total_gdp AS (
                 -- Step 3A: Sum the GDP of all European Union (EU + EURO_ZONE) countries for each year
                 SELECT
                     year, 'European Union (Calculated)' AS entity, SUM (total_gdp) AS total_gdp_ppp
                 FROM eu_country_gdp
                 GROUP BY year
            ),

            usmca_country_gdp AS (
            -- Step 4: Calculate total GDP for United States, Mexico & Canada (USMCA) using a JOIN
            SELECT ci.year,
                   ci.country_name,
                   (MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' THEN ci.value END) *
                    MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END)) AS total_gdp
            FROM country_indicators ci
                     JOIN country_classification cc ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'USMCA'
              AND ci.indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
            GROUP BY ci.year, ci.country_name
            HAVING MAX(CASE
                           WHEN ci.indicator_name = 'GDP per capita PPP (current international $)'
                               THEN ci.value END) IS NOT NULL
               AND MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END) IS NOT NULL),
            usmca_total_gdp AS (
                 -- Step 4A: Sum the GDP of all USMCA countries for each year
                 SELECT
                     year, 'USMCA (Calculated)' AS entity, SUM (total_gdp) AS total_gdp_ppp
                 FROM usmca_country_gdp
                 GROUP BY year
            )
            
-- Step 5: Combine the results
        SELECT *
        FROM brics_total_gdp
        UNION ALL
        SELECT *
        FROM asean_total_gdp
        UNION ALL
        SELECT *
        FROM eu_total_gdp
        UNION ALL
        SELECT *
        FROM usmca_total_gdp
        ORDER BY entity, year; \
        """

# Execute the query
usmca_brics_asean_eu_gdp_df = pd.read_sql_query(query2, engine)

# For a bar plot, it's best to compare a few specific years
years_for_plot = [2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]  # Choosing some representative years
plot_df = usmca_brics_asean_eu_gdp_df[usmca_brics_asean_eu_gdp_df['year'].isin(years_for_plot)]

# Pivot the data to make it suitable for a grouped bar plot and use pandas plotting
pivot_df = plot_df.pivot(index='year', columns='entity', values='total_gdp_ppp')
ax = pivot_df.plot(kind='bar', figsize=(18, 8), width=0.8, colormap='viridis')

ax.set_title('GDP Comparison: USMCA vs. BRICS vs. ASEAN vs. European Union(Calculated)', fontsize=18)
ax.set_xlabel('Year', fontsize=14)
ax.set_ylabel('Total GDP (PPP, current international $)', fontsize=14)
ax.tick_params(axis='x', rotation=0)  # Keep year labels horizontal


# Format y-axis to be more readable (in trillions)
def trillions(x, pos):
    'The two args are the value and tick position'
    return f'${x * 1e-12:1.1f}T'

formatter = FuncFormatter(trillions)
ax.yaxis.set_major_formatter(formatter)

ax.legend(title='Entity')
ax.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()


# SQL JOIN query

query3 = """
        -- Consulta para ASEAN
        WITH asean_life_gdp AS (
            SELECT 
                ci.country_name,
                MAX(CASE WHEN ci.indicator_name = 'Life expectancy at birth, total (years)' 
                        THEN ci.value END) AS life_expectancy,
                MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' 
                        THEN ci.value END) AS gdp_per_capita
            FROM country_indicators ci
            JOIN country_classification cc 
                ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'ASEAN'
            AND ci.year = 2019
            AND ci.indicator_name IN ('Life expectancy at birth, total (years)',
                                        'GDP per capita PPP (current international $)')
            GROUP BY ci.country_name
        )
        SELECT 
            country_name,
            life_expectancy,
            gdp_per_capita
        FROM asean_life_gdp
        WHERE life_expectancy > 75
        AND gdp_per_capita > 20000
        ORDER BY country_name;
        """
         
us_asean_lifeexpect_df = pd.read_sql_query(query3, engine)

query4 = """
        -- Consulta para MERCOSUR
        WITH mercosur_life_gdp AS (
            SELECT 
                ci.country_name,
                MAX(CASE WHEN ci.indicator_name = 'Life expectancy at birth, total (years)' 
                        THEN ci.value END) AS life_expectancy,
                MAX(CASE WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' 
                        THEN ci.value END) AS gdp_per_capita
            FROM country_indicators ci
            JOIN country_classification cc 
                ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'MERCOSUR'
            AND ci.year = 2019
            AND ci.indicator_name IN ('Life expectancy at birth, total (years)',
                                        'GDP per capita PPP (current international $)')
            GROUP BY ci.country_name
        )
        SELECT 
            country_name,
            life_expectancy,
            gdp_per_capita
        FROM mercosur_life_gdp
        WHERE life_expectancy > 75
        AND gdp_per_capita > 20000
        ORDER BY country_name;
        """
         
us_mercosur_lifeexpect_df = pd.read_sql_query(query4, engine)

combined_df = pd.concat([us_asean_lifeexpect_df, us_mercosur_lifeexpect_df])

print(combined_df)