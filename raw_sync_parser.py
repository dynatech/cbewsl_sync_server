import os, time
import re
import configparser
from pprint import pprint
import db_lib as dbLib
import sys
from datetime import datetime as dt

class Parser:
	def __init__(self):
		self.db = dbLib.DatabaseConnection()
		print(">> Initialize Parser...")

	def parse_raw_data(self, raw_data):
		table_reference = {
			"RiskAssessmentSummary":"risk_assessment_summary",
			"RiskAssessmentFamilyRiskProfile":"family_profile",
			"RiskAssessmentHazardData":"hazard_data",
			"RiskAssessmentRNC":"resources_and_capacities",
			"FieldSurveyLogs":"field_survey_logs",
			"SurficialDataCurrentMeasurement": "ground_measurement",
			"SurficialDataMomsSummary": "manifestations_of_movements"
		}

		for raw in raw_data:
			sender_detail = self.db.get_user_data(raw.simnum)
			if (len(sender_detail) != 0):
				sender = {
					"full_name": sender_detail[0][2]+" "+ sender_detail[0][3],
					"user_id": sender_detail[0][0],
					"account_id": sender_detail[0][1]
				}
			deconstruct = raw.data.split(":")
			key = deconstruct[0]
			actual_raw_data = deconstruct[1].split("||")
			data = []
			for objData in actual_raw_data:
				data.append(objData.split("<*>"))

			if (key == "MoMsReport"):
				print(">> Initialize MoMs Reporting...")
				self.disseminateToExperts(data[0][0],data[0][2],data[0][1],data[0][3],sender)
			else:
				result = self.db.execute_syncing(table_reference[key], data)
				self.syncing_acknowledgement(key, result, sender)

	def syncing_acknowledgement(self, key, result, sender):
		print(">> Sending sync acknowledgement...")
		sim_num_container = []
		if (len(result) == 0):
			sim_nums = self.db.get_sync_acknowledgement_recipients()
			for sim_num in sim_nums:
				sim_num_container.append(sim_num[0])

			message = "CBEWS-L Sync Ack\n\nStatus: Synced\nModule: %s " \
				"\nTimestamp: %s\nSynced by: %s (ID: %s)" % (key, 
					dt.today().strftime("%A, %B %d, %Y, %X"), sender["full_name"], sender["account_id"])

			insert_smsoutbox = self.db.write_outbox(
				message=message, recipients=sim_num_container, table='users')
			print(">> Acknowledgement sent...")
		else:
			print(">> Failed to sync data to server...")
	
	def disseminateToExperts(self, feature, feature_name, description, tos, sender):
		ct_phone = ['9175048863','9499942312']
		message = "Manifestation of Movement Report (UMI)\n\n" \
		"Time of observations: %s\n"\
		"Feature type: %s (%s)\nDescription: %s\n" % (tos, feature, feature_name, description)

		insert_smsoutbox = self.db.write_outbox(
			message=message, recipients=ct_phone, table='users')
		print(">> Acknowledgement sent...")