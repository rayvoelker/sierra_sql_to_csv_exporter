#~ this script will fetch data from the sierra postgresql database and
#~ fill a local database.

import configparser
import psycopg2
import psycopg2.extras
import csv
import os
from random import randint
from datetime import datetime, date

# debug
# import pdb

class App:

	def __init__(self):

		self.d = date.today()
		#~ open the config file, and parse the options into local vars
		config = configparser.ConfigParser()
		config.read('config.ini')

		 # get the output path from the config
		self.output_path = config['misc']['output_path']
		print('output_path: {}'.format(self.output_path))
		if not os.path.exists(self.output_path):
			os.sys.exit('ERROR: output path doesn\'t exist ... exiting')

		# set the filenames for the output csv files

		self.csv_output = os.path.join(
			self.output_path,
			self.d.strftime("%Y-%m-%d-csv_output.csv")
		)

		# the remote database connection
		self.db_connection_string = config['db']['connection_string']
		self.pgsql_conn = None


		# the number of rows to iterate over
		self.itersize = int(config['db']['itersize'])

		# open the database connections
		self.open_db_connections()

		# create the temp table
		self.create_remote_temp_tables()

		# do the local export
		self.local_export()


	#~ the destructor
	def __del__(self):
		self.close_connections()
		print("done.")


	def open_db_connections(self):
		#~ connect to the sierra postgresql server
		try:
			self.pgsql_conn = psycopg2.connect(
				self.db_connection_string)

		except psycopg2.Error as e:
			print("unable to connect to sierra database: %s" % e)


	def close_connections(self):
		print("closing database connections...")
		if self.pgsql_conn:
			if hasattr(self.pgsql_conn, 'close'):
				print("closing pgsql_conn")
				self.pgsql_conn.close()
				self.pgsql_conn = None


	def create_remote_temp_tables(self):
		"""
		if doing a large export, it's faster to create a temp table for
		staging on the server, and then export from there
		"""
		# open the sql from file ..
		# sql_string = open('temp_table.sql',
		#	mode='r',
		#	encoding='utf-8-sig').read()

		# or, just place SQL here
		sql_string = u"""
		DROP TABLE IF EXISTS temp_output
		;

		CREATE TEMP TABLE temp_output AS
		SELECT
		*
		FROM
		sierra_view.record_metadata as r

		LIMIT 1000
		"""

		with self.pgsql_conn as conn:
			with conn.cursor() as cursor:
				print('creating temp data table ...')
				cursor.execute(sql_string)
				print('done creating temp data tables.')

		cursor = None
		conn = None


	def rand_int(self, length):
		#~ simple random number generator for our named cursor
		return randint(10**(length-1), (10**length)-1)


	def gen_sierra_data(self, query):
		#~ fetch and yield self.itersize number of rows per round
		generator_cursor = "gen_cur" + str(self.rand_int(10))

		try:
			cursor = self.pgsql_conn.cursor(name=generator_cursor,
					cursor_factory=psycopg2.extras.NamedTupleCursor)
			cursor.itersize = self.itersize # sets the itersize
			cursor.execute(query)

			rows = None
			while True:
				rows = cursor.fetchmany(self.itersize)
				if not rows:
					break

				for row in rows:
					# debug
					# pdb.set_trace()
					yield row

			cursor.close()
			cursor = None

		except psycopg2.Error as e:
			print("psycopg2 Error: {}".format(e))


	def local_export(self):
		# insert the bib data
		csv_file = open(self.csv_output, 'w')
		csv_file_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
		# quoting=csv.QUOTE_MINIMAL

		print('start export')
		row_counter = 0

		sql_string = u"""
		SELECT
		*
		FROM
		temp_output
		;
		"""

		for row in self.gen_sierra_data(query=sql_string):
			row_counter += 1

			# write to csv file
			if(row_counter==1):
				# Write to the header
				# if we want to output all data to the .csv, the following may be useful
				keys = row._asdict().keys()
				# print('row keys: {}'.format(keys))
				csv_file_writer.writerow((keys))

			csv_file_writer.writerow((row))

			# commit values to the local database every self.itersize times through
			if(row_counter % self.itersize == 0):
				print('.',end='')
		csv_file.close()
		print("\ndone with bib export")


#~ run the app!
start_time = datetime.now()
print('starting import at: \t\t{}'.format(start_time))
app = App()
end_time = datetime.now()
print('finished import at: \t\t{}'.format(end_time))
print('total import time: \t\t{}'.format(end_time - start_time))
