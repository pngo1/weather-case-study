from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

def main():
	DATADIR = str(Path(__file__).parent) 
	DIALECT = 'sqlite:///'
	DB_NAME = 'weatherdata.db'
	db_uri = DIALECT      

	engine       = create_engine(db_uri + DB_NAME, echo=False)
	sqlite_conn  = engine.connect()
	if not sqlite_conn:
		print("DB connection is not OK!")
		exit()
	else:
		print("DB connection is OK.")

	try:
		sqlite_conn.execute('DROP TABLE IF EXISTS Place')
		sqlite_conn.execute('DROP TABLE IF EXISTS Observation')
		sqlite_conn.execute('DROP TABLE IF EXISTS Temperature')

		sqlite_conn.execute('CREATE TABLE Place ('
												'code INT  NOT NULL,'
												'name TEXT NOT NULL,'
												'latitude REAL NOT NULL,'
												'longitude REAL NOT NULL,'
                                                'PRIMARY KEY ('
                                                'code'
                                                ')'
												')'
												)
		sqlite_conn.execute('CREATE TABLE Observation ('
												'place INT NOT NULL,'
												'date DATE NOT NULL,'
												'rain REAL,'
												'snow REAL,'
												'airtemp REAL,'
												'groundtemp REAL,'
												'PRIMARY KEY (place, date)'
												')'
												)
		sqlite_conn.execute('CREATE TABLE Temperature ('
												'place TEXT NOT NULL,'
												'date TEXT NOT NULL,'
												'lowest REAL,'
												'highest REAL,'
												'PRIMARY KEY (place, date)'
												')'
												)
	
		df_ = pd.read_csv(DATADIR + '\weather_data_2020.csv', sep=',', quotechar='"', dtype='unicode')

		df_.insert(loc = 0, column = 'date', value = df_[['year', 'month','day']].T.agg('-'.join))     
		df_.drop(['year', 'month', 'day', 'time', 'timezone'], axis=1, inplace=True)       
		df_.rename(columns={'place_code': 'code', 'place': 'name', 'air_temperature': 'airtemp', 'ground_temperature': 'groundtemp', 'highest_temperature': 'highest', 'lowest_temperature': 'lowest'}, inplace= True)
		

		df_['rain'] = df_['rain'].astype('float64').replace(-1.0,0)
		df_['snow'] = df_['snow'].astype('float64').replace(-1.0,0)
		df_['latitude'] = df_['latitude'].astype('float64')
		df_['longitude'] = df_['longitude'].astype('float64')
		df_['lowest'] = df_['lowest'].astype('float64')    
		df_['highest'] = df_['highest'].astype('float64')              
		df_ = df_.drop_duplicates(subset=['date','code'], keep='first')
		

		df_[['code','name','latitude','longitude']].drop_duplicates().to_sql('Place', engine, if_exists='append', index=False)
		df_.rename(columns={'code': 'place'}, inplace= True)
		df_[['place','date','rain','snow','airtemp','groundtemp']].drop_duplicates().to_sql('Observation', engine, if_exists='append', index=False)
		df_[['place','date','lowest','highest']].drop_duplicates().to_sql('Temperature', engine, if_exists='append', index=False)
		print(df_.head)

        #Exercise 3

        #Exercise 3a:
        #Find the number of snowy days on each location
		sql_ = """
                    SELECT name,
                           COUNT(date) AS snowday
                    FROM Place,
                         Observation
                    WHERE Place.code = Observation.place AND 
                          snow IS NOT NULL AND 
                          snow != 0
                    GROUP BY place;	

					"""
		ans_df = pd.read_sql_query(sql_,sqlite_conn)
		print("The number of snowy days on each location: ")
		print(ans_df)

        #Which location (name) has had most snowy days? 
		sql_ = """
        SELECT month, MAX(snow1) 
           FROM (
           SELECT substr(date, 6, 1) AS month,
                  SUM(snow) AS snow1
             FROM place,
                  observation
            WHERE place = code AND 
                  name = 'Utsjoki' AND 
                  snow IS NOT NULL
            GROUP BY month
       );
					"""
		ans1_df = pd.read_sql_query(sql_,sqlite_conn)
		print("The place that has had most snowy days is: ")
		print(ans1_df)

        #Exercise 3b:
        #Calculate the sample correlation coefficient between these two attributes
		sql4_ = """
                SELECT place, highest, lowest 
                FROM Temperature
                GROUP BY Temperature.place,Temperature.date

					"""
		df4 = pd.read_sql_query(sql4_,sqlite_conn)
		ans3_df = pd.DataFrame(df4, columns=['place', 'highest', 'lowest'])
		print('The sample correlation coefficient between these two attributes: ')
		correlation = ans3_df.groupby('place')[['highest', 'lowest']].corr().unstack().iloc[:, 1]	
		print(correlation)		
		print('This value shows the positive correlation')

        #Exercise 3c:
        #Find out the correlation between average temperature and latitude of the location.
		sql5 = """
		SELECT place, AVG(airtemp) as avg_temp, latitude
		FROM Place,Observation
		WHERE Place.code = Observation.place
		GROUP BY Observation.place
		"""
		df5 = pd.read_sql_query(sql5, sqlite_conn)
		ans5_df = pd.DataFrame(df5, columns=['place', 'avg_temp', 'latitude'])
		correlation1 = ans5_df['avg_temp'].corr(ans5_df['latitude'])
		print('Correlation between the year average air temperature and latitude:\n')
		print(correlation1)

        #Exercise 3d:
        #For each location, use myplotlib to plot the number of rainy days for each month as a bar plot.
		sql6 = """
        SELECT
         place, strftime('%m', date) as month, count(rain) as rainydays
         FROM Observation
         WHERE rain > 0.0
         GROUP BY
         strftime('%m', date), place
    """
		df6 = pd.read_sql_query(sql6, sqlite_conn)
		ans6_df= pd.DataFrame(df6, columns=['month', 'place', 'rainydays'])
		pivot1 = pd.pivot_table(ans6_df,
                             values='rainydays', index=['month'], columns=ans6_df.place.values)
		plot_1 = pivot1.plot.bar(title='Number of rainy days for each month', legend=True, fontsize=12)
		plot_1.set_ylabel("days", fontsize=10)
		#plt.show()

    #Exercise 3e:
    #For each location, plot the average temperature throughout the year. You may plot all the graphs into the same Figure.
		sql7 = """
         SELECT
          place, date , airtemp
          FROM Observation
          GROUP BY
          place, date
    """
		df7 = pd.read_sql_query(sql7, sqlite_conn)
		ans7_df= pd.DataFrame(df7, columns=['place', 'date', 'airtemp'])
		ans7_df['date'] = pd.to_datetime(ans7_df['date']).dt.date
		pivot2 = pd.pivot_table(ans7_df,
                              values='airtemp', index=['date'], columns=ans7_df.place.values)
		plot2 = pivot2.plot.line(title='Average Air Temperature 2020', legend=True, fontsize=12)
		plot2.set_ylabel("Celcius degree", fontsize=10)

		plt.show()

	except Exception as e:
		print(f'FAILED due to: {str(e)}')
	finally:
		sqlite_conn.close()

main()