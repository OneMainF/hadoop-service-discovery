#!/usr/bin/python

##section:Dependencies
######################################################################################
##extdep:socket
import socket
##extdep:urllib2
import urllib2
##extdep:json
import json
##extdep:os.path
import os.path
##extdep:filecmp
import filecmp
##extdep:subprocess
import subprocess
##import:re
import re
##extdep:argparse
import argparse

##section:Variables
######################################################################################
hApps = {} ##vard:Hash for apps
hBackends = {} ##vard:Hash for backends
hAppConfig = {} ##vard:App config hash
sOutput = "" ##stmt:Output from query
sThisBaseConfigFile = "hahadoop.cfg" ##vard:Base config
sFinalConfig = "/opt/om/tmp/hahadoop.cfg" ##vard:Final config
sActualConfig = "/etc/haproxy/haproxy.cfg" ##vard:Actual haproxy config file
sAppConfig = "appConfig.json" ##vard:App config

##section:Main
######################################################################################
##stmt:Parse args
parser = argparse.ArgumentParser()
parser.add_argument("host")
args = parser.parse_args()
sThisHost = str(args.host)

##if:Base config exist?
if os.path.isfile(sThisBaseConfigFile):
        ##stmt:Yes, read it

        ##loop:Read base config
        for sLine in open(sThisBaseConfigFile):
                sOutput += sLine
else:
        ##else:Base config file not found
        ##stmt:Exit script
        print "Cannot find " + sThisBaseConfigFile
        exit(1)

##stmt:Read in appConfig
with open(sAppConfig) as oFile:
	##stmt:Get next line
	hAppConfig = json.load(oFile)

##stmt:Get all apps
oJson = json.loads(urllib2.urlopen("http://" + sThisHost + ":8088/ws/v1/cluster/apps").read())

##loop:Get all jobs
for sApp in oJson["apps"]["app"]:
	##stmt:Get next app

	##if:Check if the job is running
	if sApp["state"] == "RUNNING":
		##stmt:Running job, get name
		sThisAppName = sApp["name"]

		##loop:Get our list of apps
		for sConfiguredApps in hAppConfig:
			##stmt:Get next app

			##if:Is this one of our apps?
			if sThisAppName == sConfiguredApps["name"]:
				##stmt:Yes, get more info about it

				##stmt:Get tracking URL, use it to get the containers
				sThisTrackingURL = sApp["trackingUrl"] + "ws/v1/slider/application/live/containers"
				oTrackingContent = json.loads(urllib2.urlopen(sThisTrackingURL).read())

				##loop:Get the container info
				for sTrackingContentKey in oTrackingContent:
					##stmt:Loop though the container

					##if:Is this the slider-appmaster
					if oTrackingContent[sTrackingContentKey]["component"] == "slider-appmaster":
						##stmt:Get the host URL
						sThisWorkerURL = oTrackingContent[sTrackingContentKey]["hostURL"]

						##stmt:Query the host URL and get the worker
						sRegistryURL = sThisWorkerURL + "/ws/v1/slider/publisher/slider/componentinstancedata"
						oRegJson = json.loads(urllib2.urlopen(sRegistryURL).read())

						##loop:Get all services from the registry
						for sServices in oRegJson["entries"]:
							##stmt:Get next service and parse the name
							aService = sServices.split(".")
							sTempContainer = aService[0]
							sService = aService[1]

							for sAppComponent in sConfiguredApps["components"]:
								if sAppComponent["name"] == sService:
									##stmt:Get service name and add it to the backend hash
									sTempKey = sThisAppName + "-" + sService + "-" + sAppComponent["ipaddress"] + ":" + sAppComponent["frontendport"]
									hApps[sTempKey] = 1
									hBackends[oRegJson["entries"][sServices]] = sTempKey


##loop:Get all of the apps from the apps hash
for sDiscoveredApps in hApps:
	##stmt:Get next app
	aTemp = sDiscoveredApps.split("-")

	##stmt:Build front end config
	sOutput += "frontend " + aTemp[0] + "-" + aTemp[1] + "\n"

	##stmt:Setup port
	sOutput += "\tbind " + aTemp[2] + "\n"

	##stmt:Build backend
	sOutput += "\tdefault_backend " + aTemp[0] + "-" + aTemp[1] + "-backend\n\n"
	sOutput += "backend " + aTemp[0] + "-" + aTemp[1] + "-backend\n"
	iCounter = 0

	##loop:Get all backends associated with this app
	for sBackEndHash in hBackends:
		##stmt:Get next backend

		##if:Is this our app?
		if sDiscoveredApps == hBackends[sBackEndHash]:
			##stmt:Yes, add it to the backend
			sOutput += "\tserver " + sDiscoveredApps + "" + str(iCounter) + " " + sBackEndHash + "\n" 
			iCounter = iCounter + 1

	##stmt:Add a line break
	sOutput += "\n"

##stmt:Write file
fFile = open(sFinalConfig, "w+")
fFile.write(sOutput)
fFile.close()

##if:Are the orginal and new files different?
if filecmp.cmp(sFinalConfig, sActualConfig) == False:
	##stmt:Yes, update the haproxy config
	fFile = open(sActualConfig, "w+")
	fFile.write(sOutput)
	fFile.close()

	##stmt:Restart haproxy
	subprocess.call(["/usr/bin/systemctl", "reload", "haproxy"])

