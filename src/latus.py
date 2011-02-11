# Copyright 2006 Uberan - All Rights Reserved

import argparse
import threading
import logging

import sleekxmpp

logging.basicConfig(level = logging.INFO,
                    format = "%(levelname)-8s %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("password")
args = parser.parse_args()


jid = "pthatcher@gmail.com"
# *** hack
password = "".join(chr(ord(c)+1) for c in args.password)
client = sleekxmpp.ClientXMPP(jid, password)
client.registerPlugin("xep_0030")  # Service Discovery
client.registerPlugin("xep_0004")  # Data Forms
client.registerPlugin("xep_0060")  # PubSub
client.registerPlugin("xep_0199")  # XMPP Ping

def on_start(event):
  client.getRoster()
  print ("client", client)
  #print ("roster", client.roster)
  for name, details in client.roster.viewitems():
    print "{0:30}: {1[name]}, {1[presence]}".format(name, details)
  client.sendPresence()

def on_message(msg):
  # check type
  # can do msg.reply().send() ?
  print repr(msg)

client.add_event_handler("session_start", on_start)
client.add_event_handler("message", on_message)

if client.connect(("talk.google.com", 5222)):
  client.process(threaded = False)
else:
  print "Couldn't connect"

                           
# Need to login, get peer info, and get peer info of others
#  get all full JIDs of buddies?

# directory = HttpJsonLatusDirectory(
#   config.directoryName, config.directoryURL, 
#   crypto.deserialize_public_key(config.directoryPublicKey),
#   LatusFileCache(services.fs, config.latusCacheDirectory))
# private_ip, private_port = (Utils.get_local_ip(), config.connectionPort)
# peer = Peer(directory)
# network = Network(map_port_description = "ShareEver")
# fs = get_file_system(config.useOSTrash, config.customTrashFolder)
# log = ???

# connection_listener = ThreadedSocketServerListener(
#  "connection listener", config.connectionPort)
# rendezvous_listener = ThreadedSocketServerListener(
#   "rendezvous listener", config.rendezvousPort)
# start/stop:
#  peer, connection_listener, rendezvous_listener

# TODO: write persistent info (not config!)
#  connection port
#  peerid
#  crypto keys?
# TODO: debug consoloe: code.interact(
#  "ShareEver Debugging Console", local = {"peer" : peer}
