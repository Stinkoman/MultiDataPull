#!/usr/bin/python3

from config.apikey import x_cisco_meraki_apikey, orgid
from config.dbinfo import dbusername, dbpassword, dbname, dbtype, dbport, dbserver
import meraki
import pandas as pd
import numpy as np
import sqlalchemy as db
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine, exists, func
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from time import sleep

mk = meraki.DashboardAPI(api_key=x_cisco_meraki_apikey,print_console=False)
dbConnectString = dbtype + "://" + dbusername + ":" + dbpassword + "@" + dbserver + ":" + dbport + "/" + dbname
db = create_engine(dbConnectString)

Base = declarative_base()

class Network(Base):
    __tablename__   =       "networks"
    id              =       Column(String(20),primary_key=True)
    organizationid  =       Column(Integer)
    name            =       Column(String(80))
    timezone        =       Column(String(20))
    tags            =       Column(String(15))
    producttypes    =       Column(String(100))
    networktype     =       Column(String(20))
    lastclientcheck =       Column(DateTime)

#networkProperties = [p for p in dir(Network) if isinstance(getattr(Network,p),property)]

class ClientDevice(Base):
    __tablename__   =       "clientdevices"
    instance        =       Column(Integer,primary_key=True)
    clientid        =       Column(String(10))
    queryperiodstart=       Column(DateTime)
    queryperiodend  =       Column(DateTime)
    mac             =       Column(String(17))
    description     =       Column(String(80))
    ip              =       Column(String(15))
    ip6             =       Column(String(46))
    ip6local        =       Column(String(46))
    user            =       Column(String(80))
    firstseen       =       Column(DateTime)
    lastseen        =       Column(DateTime)
    manufacturer    =       Column(String(80))
    recentdevicemac =       Column(String(17))
    ssid            =       Column(String(50))
    vlan            =       Column(Integer)
    switchport      =       Column(String(5))
    usagesent       =       Column(Integer)
    usagerecv       =       Column(Integer)
    status          =       Column(String(15))
    notes           =       Column(String(150))
    sminstalled     =       Column(String(5))
    grouppolicy8021x=       Column(String(100))
    networkid       =       Column(String(20),ForeignKey('networks.id'))
    network         =       relationship(Network)

#clientDeviceProperties = [p for p in dir(ClientDevice) if isinstance(getattr(ClientDevice),property)]

if not db.dialect.has_table(db, Network.__tablename__):
    print ("Did not find " + Network.__tablename__ + " table. Creating.")
    Network.__table__.create(bind=db)
        

if not db.dialect.has_table(db, ClientDevice.__tablename__):
    print ("Did not find " + ClientDevice.__tablename__ + " table. Creating.")
    ClientDevice.__table__.create(bind=db)

dbsession = sessionmaker(bind=db)
session = dbsession()

orgs = mk.organizations.getOrganization(orgid)

# TODO: work some of the scheduling into decorators
# For the time being, the networks enumeration will only run one time
# at the beginning of the script.

mkdbnetworks = mk.networks.getOrganizationNetworks(orgs['id'])

dbChanged = False
for network in mkdbnetworks:
    if not session.query(exists().where(Network.id==network['id'])).scalar():
        print("Adding network " + network['name'] + " to DB.")
        newNetwork = Network(id=network['id'],
            organizationid=network['organizationId'],
            name=network['name'],
            timezone=network['timeZone'],
            tags=str(network['tags']),
            producttypes=str(network['productTypes']),
            networktype=network['type'],
            lastclientcheck=datetime(1970,1,1)
        )
        session.add(newNetwork)
        dbChanged = True
    if dbChanged:
        session.commit()

#it's a service, so run forever

while True:
    networks = session.query(Network).all()
    now = datetime.now()
    branchNetworks = [network for network in networks if network.networktype != "systems manager"] 
    for network in branchNetworks:
        if (now > network.lastclientcheck + timedelta(hours=1)):
            print("Last client check occurred more than one hour ago for " + network.name + " - refreshing check.")
            if (network.lastclientcheck + timedelta(days=30)) < now:
                print("Timespan is too large. Setting to 1 hour.")
                timeSpan = int((now - timedelta(hours=1)).timestamp())
            else:
                timeSpan = int((network.lastclientcheck).timestamp())
            clientsThisHour = mk.clients.getNetworkClients(networkId=network.id,
                                                            total_pages="all",
                                                            t0=timeSpan)
            
            dbclientinstancecount = session.query(func.max(ClientDevice.instance)).scalar()
            if dbclientinstancecount is None:
                dbclientinstancecount = int(0)
            for eachClient in clientsThisHour:
                print("Adding client to query period block - " + str(eachClient['description']))
                dbclientinstancecount += 1
                newClient = ClientDevice(instance=dbclientinstancecount,
                        clientid = eachClient['id'],
                        queryperiodstart = network.lastclientcheck,
                        queryperiodend = now,
                        mac = eachClient['mac'],
                        description = eachClient['description'],
                        ip = eachClient['ip'],
                        ip6 = eachClient['ip6'],
                        ip6local = eachClient['ip6Local'],
                        user = eachClient['user'],
                        firstseen = parser.parse(eachClient['firstSeen']),
                        lastseen = parser.parse(eachClient['lastSeen']),
                        manufacturer = eachClient['manufacturer'],
                        recentdevicemac = eachClient['recentDeviceMac'],
                        ssid = eachClient['ssid'],
                        vlan = eachClient['vlan'],
                        switchport = eachClient['switchport'],
                        usagesent = eachClient['usage']['sent'],
                        usagerecv = eachClient['usage']['recv'],
                        status = eachClient['status'],
                        notes = eachClient['notes'],
                        sminstalled = eachClient['smInstalled'],
                        grouppolicy8021x = eachClient['groupPolicy8021x'],
                        networkid = network.id
                        )
                session.add(newClient)
            print("The network is " + network.name + " and the last client check is " + str(network.lastclientcheck))
            network.lastclientcheck = now
            print("now the last client check is " + str(network.lastclientcheck))
            session.commit()

    sleep(5)



        

