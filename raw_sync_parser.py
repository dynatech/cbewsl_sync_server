import os, time
import re
import configparser
from pprint import pprint
import db_lib as dbLib
import sys

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
			"SurficialDataCurrentMeasurement": "ground_measurement"
		}

		for raw in raw_data:
			deconstruct = raw.data.split(":")
			key = deconstruct[0]
			actual_raw_data = deconstruct[1].split("||")
			data = []
			for objData in actual_raw_data:
				data.append(objData.split("<*>"))
			result = self.db.execute_syncing(table_reference[key], data)
			self.syncing_acknowledgement(key, result)

	def syncing_acknowledgement(self, key, result):
		print(">> Sending sync acknowledgement...")
		print(key)
		print(result)