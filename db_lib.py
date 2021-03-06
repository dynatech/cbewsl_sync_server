import MySQLdb
import configparser
from datetime import datetime as dt
from datetime import timedelta as td
from pprint import pprint
import sys
import re
import time
import hashlib

class DatabaseCredentials:
	def __new__(self):
		config = configparser.ConfigParser()
		config.read('/home/pi/cbewsl_sync_server/gsmserver_dewsl3/utils/config.cnf')
		config["CBEWSL_DB_CREDENTIALS"]
		return config


class DatabaseConnection:

	db_cred = None

	def __init__(self):
		self.db_cred = DatabaseCredentials()

	def get_all_outbox_sms_from_db(self, table, send_status, gsm_id):
		if not table:
			raise ValueError("No table definition")

		while True:
			try:
				db, cur = self.db_connect(
					self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
				query = ("select t1.stat_id,t1.mobile_id,t1.gsm_id,t1.outbox_id,"
						 "t2.sms_msg from "
						 "smsoutbox_%s_status as t1 "
						 "inner join (select * from smsoutbox_%s) as t2 "
						 "on t1.outbox_id = t2.outbox_id "
						 "where t1.send_status < %d "
						 "and t1.send_status >= 0 and t1.send_status < 6 "
						 "and t1.gsm_id = %d LIMIT 100") % (table[:-1], table, send_status, gsm_id)

				a = cur.execute(query)
				out = []
				if a:
					out = cur.fetchall()
					db.close()
				return out

			except MySQLdb.OperationalError:
				print('10.')
				time.sleep(20)

	def get_all_logger_mobile(self, sim_num):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
			query = ("SELECT * FROM (SELECT "
						"t1.mobile_id, t1.sim_num, t1.gsm_id "
						"FROM "
						"logger_mobile AS t1 "
						"LEFT OUTER JOIN "
						"logger_mobile AS t2 ON t1.sim_num = t2.sim_num "
						"AND (t1.date_activated < t2.date_activated "
						"OR (t1.date_activated = t2.date_activated "
						"AND t1.mobile_id < t2.mobile_id)) "
						"WHERE "
						"t2.sim_num IS NULL "
						"AND t1.sim_num IS NOT NULL) as logger_mobile WHERE sim_num like '%"+sim_num[:10]+"%'")

			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError:
			time.sleep(20)

	def get_all_user_mobile(self, sim_num, mobile_id_flag=False):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
			if mobile_id_flag == False:
				query = "select mobile_id, sim_num, gsm_id from user_mobile where sim_num like '%"+sim_num+"%'"
			else:
				query = "select mobile_id, sim_num, gsm_id from user_mobile where mobile_id = '" + \
					str(sim_num)+"'"
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)

	def get_user_data(self, sim_num):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])

			query = "SELECT users.user_id, account_id, first_name, last_name from commons_db.users" \
			" INNER JOIN comms_db.user_mobile ON users.user_id = user_mobile.user_id " \
			"INNER JOIN commons_db.user_accounts ON users.user_id = user_accounts.user_fk_id WHERE user_mobile.sim_num like '%"+sim_num[-10:]+"%'"
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)


	def get_sync_acknowledgement_recipients(self):
		query = "SELECT sim_num FROM user_accounts INNER JOIN comms_db.user_mobile ON  user_fk_id = user_id WHERE role <> 1"
		result = self.execute_commons_db(query)
		return result

	def write_inbox(self, msglist='', gsm_info=''):
		if not msglist:
			raise ValueError("No msglist definition")

		if not gsm_info:
			raise ValueError("No gsm_info definition")

		ts_stored = dt.today().strftime("%Y-%m-%d %H:%M:%S")

		gsm_id = gsm_info['id']


		sms_id_ok = []
		sms_id_unk = []
		ts_sms = 0

		for msg in msglist:
			ts_sms = msg.dt
			sms_msg = msg.data
			query_users = self.write_raw_data(msg, gsm_id, ts_sms, ts_stored, sms_msg)
		
	def execute_syncing(self, table_reference = '', data=[]):
		print(table_reference)
		print(data)
		result = None
		for entry in data:
			if '1' in entry[2]:
				result = self.sync_new_data(table_reference, entry)
			elif '2' in entry[2]:
				result = self.sync_modified_data(table_reference, entry)
			else:
				print(">> Old data. Ignoring...")
		return result

	def sync_modified_data(self, table='', data=[]):
		update_set = ""
		counter = 0
		data_counter = 1

		del data[2]
		del data[1]
		
		fetch_columns = self.get_column_names(table)
		column_names = self.get_column_names(table)
		
		set_id = column_names[0]
		del column_names[0]

		for column in column_names:
			if counter == 0:
				update_set = ("SET %s='%s'") % (column, data[data_counter])
				counter += 1
			else:
				update_set = update_set+", %s='%s'" % (column, data[data_counter])
			data_counter +=1

		query = "UPDATE %s %s WHERE %s = %s" % (table, update_set, set_id, data[0])
		return self.execute_commons_db(query)

	def sync_new_data(self, table='', data=[]):
		value_container = ""
		counter = 0
		del data[2]
		del data[1]
		for values in data:
			if counter == 0:
				value_container = "'"+values+"'"
				counter += 1
			else:
				value_container = value_container+",'"+values+"'"

		if (table != "ground_measurement"):
			query = "INSERT INTO %s VALUES (%s)" % (table, value_container)
		else:
			print("GROUND MEASUREMENT")
		print(query)
		return self.execute_commons_db(query)

	def get_column_names(self, table=''):
		column_names = []
		query = "SHOW columns FROM %s;" % table
		column_details = self.execute_commons_db(query)
		for column in column_details:
			column_names.append(column[0])
		return column_names

	def write_raw_data(self, msg, gsm_id, ts_sms, ts_stored, sms_msg):
		users_count = 0
		query_raw = ("insert into raw_data_received (raw_data, ts_received, ts_stored, mobile_id, parsed) values ")
		user_mobile_sim_nums = self.get_all_user_mobile(msg.simnum[:10])

		if len(user_mobile_sim_nums) != 0:
			user_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num,
									gsm_id) in user_mobile_sim_nums}

			if msg.simnum in user_mobile_sim_nums.keys():
				query_raw += "('%s','%s','%s',%d, 0)," % (sms_msg, ts_sms, ts_stored, user_mobile_sim_nums[msg.simnum])
				users_count += 1
			
			query_raw = query_raw[:-1]
			print(">> Raw data received...")
			result = self.write_raw_to_db(query=query_raw)
			return result
		else:
			return -1

	def write_raw_to_db(self, query, last_insert_id=False):
		ret_val = None
		db, cur = self.db_connect(
			self.db_cred['CBEWSL_DB_CREDENTIALS']['db_cbewsl_raw'])

		try:
			a = cur.execute(query)
			db.commit()
			if last_insert_id:
				b = cur.execute('select last_insert_id()')
				b = str(cur.fetchone()[0]) 
				ret_val = b
			else:
				ret_val = a

		except IndexError:
			print("IndexError on ")
			print(str(inspect.stack()[1][3]))
		except (MySQLdb.Error, MySQLdb.Warning) as e:
			print(">> MySQL error/warning: %s" % e)
			print("Last calls:")
			for i in range(1, 6):
				try:
					print("%s," % str(inspect.stack()[i][3]),)
				except IndexError:
					continue
			print("\n")

		finally:
			db.close()
			return ret_val

	def write_to_db(self, query, last_insert_id=False):
		ret_val = None
		db, cur = self.db_connect(
			self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])

		try:
			a = cur.execute(query)
			db.commit()
			if last_insert_id:
				b = cur.execute('select last_insert_id()')
				b = str(cur.fetchone()[0]) 
				ret_val = b
			else:
				ret_val = a

		except IndexError:
			print("IndexError on ")
			print(str(inspect.stack()[1][3]))
		except (MySQLdb.Error, MySQLdb.Warning) as e:
			print(">> MySQL error/warning: %s" % e)
			print("Last calls:")
			for i in range(1, 6):
				try:
					print("%s," % str(inspect.stack()[i][3]),)
				except IndexError:
					continue
			print("\n")

		finally:
			db.close()
			return ret_val

	def read_db(self, query):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)

	def db_connect(self, schema):
		try:
			db = MySQLdb.connect(self.db_cred['CBEWSL_DB_CREDENTIALS']['host'],
								 self.db_cred['CBEWSL_DB_CREDENTIALS']['user'],
								 self.db_cred['CBEWSL_DB_CREDENTIALS']['password'], schema)
			cur = db.cursor()
			return db, cur
		except TypeError:
			print('Error Connection Value')
			return False
		except MySQLdb.OperationalError as mysqle:
			print("MySQL Operationial Error:", mysqle)
			return False
		except (MySQLdb.Error, MySQLdb.Warning) as e:
			print("MySQL Error:", e)
			return False

	def update_sent_status(self, table='', status_list='', resource="sms_data"):
		if not table:
			raise ValueError("No table definition")

		if not status_list:
			raise ValueError("No status list definition")

		query = ("insert into smsoutbox_%s_status (stat_id,send_status,ts_sent,"
				 "outbox_id,gsm_id,mobile_id) values ") % (table[:-1])

		for stat_id, send_status, ts_sent, outbox_id, gsm_id, mobile_id in status_list:
			query += "(%d,%d,'%s',%d,%d,%d)," % (stat_id, send_status, ts_sent,
												 outbox_id, gsm_id, mobile_id)

		query = query[:-1]
		query += (" on duplicate key update stat_id=values(stat_id), "
				  "send_status=send_status+values(send_status),ts_sent=values(ts_sent)")
		self.write_to_db(query=query, last_insert_id=False)

	def get_gsm_info(self, gsm_id):
		gsm_dict = {}
		query = "SELECT * FROM gsm_modules where gsm_id = '" + \
			str(gsm_id)+"';"
		gsm_info = self.read_db(query)  # Refactor this
		for gsm_id, gsm_server_id, gsm_name, sim_num, network, port, pwr, rng, module_type in gsm_info:
			gsm_dict['gsm_id'] = gsm_id
			gsm_dict['gsm_server_id'] = gsm_server_id
			gsm_dict['gsm_name'] = gsm_name
			gsm_dict['sim_num'] = sim_num
			gsm_dict['network'] = network
			gsm_dict['port'] = port
			gsm_dict['pwr'] = pwr
			gsm_dict['rng'] = rng
			gsm_dict['module_type'] = module_type
		container = {gsm_id: gsm_dict}
		return container

	def write_outbox(self, message=None, recipients=None, table=None):
		tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")

		if not message:
			print("No message specified for sending, skipping...")
			return -1

		if not recipients:
			print("No recipients specified for sending, skipping...")
			return -1
		for recipient in recipients:
			recipient = self.get_all_user_mobile(recipient[:10])
		
			query = ("insert into smsoutbox_%s (ts_written,sms_msg) VALUES "
				"('%s','%s')") % (table,tsw,message)
			outbox_id = self.write_to_db(query=query, last_insert_id=True)
			print(query)
			query = ("INSERT INTO smsoutbox_%s_status (outbox_id,mobile_id,gsm_id)"
					" VALUES ") % (table[:-1])
			print(query)

			for rcpt in recipient:
				tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
				try:
					query += "(%s, %s, %s)," % (outbox_id, rcpt[0], rcpt[2])
				except KeyError:
					print (">> Error: Possible key error for", r)
					continue
			query = query[:-1]
			self.write_to_db(query=query, last_insert_id=False)
		return 0

	def get_inbox(self, host='local',read_status=0,table='loggers',limit=200,
	resource="sms_data"):
		db, cur = dbio.connect(host=host, resource=resource)

		if table in ['loggers','users']:
			tbl_contacts = '%s_mobile' % table[:-1]
		else:
			raise ValueError('Error: unknown table', table)
		
		while True:
			try:
				query = ("select inbox_id,ts_sms,sim_num,sms_msg from "
					"(select inbox_id,ts_sms,mobile_id,sms_msg from smsinbox_%s "
					"where read_status = %d order by inbox_id desc limit %d) as t1 "
					"inner join (select mobile_id, sim_num from %s) as t2 "
					"on t1.mobile_id = t2.mobile_id ") % (table, read_status, limit,
					tbl_contacts)

				a = cur.execute(query)
				out = []
				if a:
					out = cur.fetchall()
				return out

			except MySQLdb.OperationalError:
				print ('9.',)
				time.sleep(20)

	def write_csq(self, gsm_id, datetime, csq):
		query = "INSERT INTO gsm_csq_logs VALUES (0, %d, '%s', %d)" % (gsm_id, datetime, csq)
		self.write_to_db(query=query, last_insert_id=False)

	def validateCBEWSLAccount(self, msg):
		status = False
		if re.search("validate", str(msg), re.IGNORECASE) and len(str(msg)) == 15:
			explode = msg.split(' ')
			if len(explode) == 3:
				status = True

		return status

	def execute_commons_db(self, query, last_insert_id = False):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_commons'])
			a = cur.execute(query)
			result = []
			db.commit()
			if last_insert_id:
				b = cur.execute('select last_insert_id()')
				b = str(cur.fetchone()[0]) 
				result = b
			else:
				if a:
					result = cur.fetchall()
			db.close()
			return result
		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)

	def insertNewAccountCBEWS(self, msg):
		# Needs validation
		parts = msg.split(' ')
		query = "SELECT * FROM pending_accounts WHERE validation_code = '"+parts[1]+"'"
		pending_account = self.execute_commons_db(query)

		for user in pending_account:
			user_query = "INSERT INTO users VALUES (0, 'NA', '%s', 'NA', '%s', 'NA', '%s', '%s', 1)" % (user[3], user[4], user[5], user[6])
			last_insert_user_id = self.execute_commons_db(user_query, last_insert_id=True)

			encode_password = str.encode(user[2])
			hash_object = hashlib.sha512(encode_password)
			hex_digest_password = hash_object.hexdigest()
			password = str(hex_digest_password)

			user_account_query = "INSERT INTO user_accounts VALUES (0, '%s', '%s', '%s', 1, '')" % (last_insert_user_id, user[1], password)
			insert_new_user_account = self.execute_commons_db(user_account_query)

			user_mobile_query = "INSERT INTO user_mobile VALUES (0, '%s', '%s', 1, 1, 1)" % (last_insert_user_id, user[8])
			self.write_to_db(query=user_mobile_query)

			delete_pending_account_query = "DELETE FROM pending_accounts WHERE pending_account_id = '"+str(user[0])+"'"
			delete_pending_account = self.execute_commons_db(delete_pending_account_query)
		self.write_to_db(query=query, last_insert_id=False)