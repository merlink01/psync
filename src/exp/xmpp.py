import sleekxmpp

import argparse
import logging
import code

logging.basicConfig(level = logging.INFO,
                    format = "%(levelname)-8s %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("password")
args = parser.parse_args()


jid = "pthatcher@gmail.com/02044160"
# hack for typing password
password = "".join(chr(ord(c)+1) for c in args.password)
client = sleekxmpp.ClientXMPP(jid, password)
# Interesting plugins:
# xep-009: RPC
# xep-030: Service Discovery
# xep-050: Ad-Hoc commands
# xep-060: PubSub
# xep-085: Chat state
# xep-199: Ping
client.registerPlugin("xep_0030")  # Service Discovery
client.registerPlugin("xep_0004")  # Data Forms
client.registerPlugin("xep_0060")  # PubSub
client.registerPlugin("xep_0199")  # XMPP Ping

# To make your own plugin, you much make a stream event handler like this:
# from https://github.com/fritzy/SleekXMPP/wiki/Event-Handlers
# self.registerHandler(
#     Callback('Example Handler',
#              MatchXPath('{%s}iq/{%s}task' % (self.default_ns, Task.namespace)),
#              self.handle_task))

# presence/subscription handled like so:
# https://github.com/fritzy/SleekXMPP/wiki/Stanzas:-Presence

# creating our own stanzas:
# https://github.com/fritzy/SleekXMPP/wiki/Stanza-Objects

def on_start(event):
  client.getRoster()
  #print ("roster", client.roster)
  for name, details in client.roster.viewitems():
    print "{0:30}: {1[name]}, {1[presence]}".format(name, details)
  client.sendPresence()

def on_message(msg):
  # check type
  # can do msg.reply().send() ?
  print repr(msg)

# Events: connected, changed_status(presence), disconnected,
# failed_auth, got_online(presence), got_offline(presence),
# on_message, presence_available, presence_error,
# presence_unavailable, roster_update, session_start
client.add_event_handler("session_start", on_start)
client.add_event_handler("message", on_message)

if client.connect(("talk.google.com", 5222)):
  print "logged in"
  #client_job = gevent.spawn(client.process)
  #gevent.sleep(1)
  client.process(threaded=True)
  code.interact("Debug (use cient)", local = {"client": client})
  # client has:
  # .address (server address)
  # .boundjid
  #   .bare
  #   .full
  # .roster
else:
  print "Couldn't connect"

                           
# Need to login, get peer info, and get peer info of others
#  get all full JIDs of buddies?
