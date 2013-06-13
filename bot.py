#!/usr/bin/env python

from twisted.words.protocols import irc
from twisted.internet import reactor, protocol, task, defer
from twisted.python import log
from twisted.enterprise import adbapi
import ConfigParser
import MySQLdb as mdb

import time, sys

class P2000Bot(irc.IRCClient):
    """A p2000 IRC bot."""
    
    nickname = "p2000"
    lineRate = .5 
    
    def connectionMade(self):
        self.factory.clientConnectionMade(self)
        irc.IRCClient.connectionMade(self)

    def connectionLost(self, reason):
        irc.IRCClient.connectionLost(self, reason)

    # callbacks for events

    def signedOn(self):
        self.join(self.factory.channel)

    def privmsg(self, user, channel, message):
        nick, _, host = user.partition('!')
        message = message.strip()
        if not message.startswith('!'): # not a trigger command
            return # do nothing
        command, sep, rest = message.lstrip('!').partition(' ')
        print "%s: %s %s"%(nick,command,message)
        func = getattr(self, 'command_' + command, None)
        if func is None:
            return
        d = defer.maybeDeferred(func, rest)
        d.addErrback(self._show_error)
        if channel == self.nickname:
            d.addCallback(self._send_message, nick)
        else:
            d.addCallback(self._send_message, channel, nick)

    def action(self, user, channel, msg):
        user = user.split('!', 1)[0]

    def _show_error(self, failure):
        return failure.getErrorMessage()

    def _send_message(self, msg, target, nick=None):
        if nick:
            msg = '%s, %s' % (nick, msg)
        self.msg(target, msg)

    # irc callbacks

    def irc_NICK(self, prefix, params):
        old_nick = prefix.split('!')[0]
        new_nick = params[0]

    def alterCollidedNick(self, nickname):
        return nickname + '^'

    # irc commands

    def command_test(self,message):
        return "Everting is working fine"

    def command_say(self,message):
        return message

    def command_search(self,msg):
        cursor = self.factory.db.cursor()
        cursor.execute("""SELECT message_id,timestamp,cap,message FROM messages WHERE message LIKE ('%%?%%') ORDER BY  `messages`.`timestamp` ASC LIMIT 5""" , (msg))
        _return = ""
        if cursor.rowcount == 0:
            return "Nothing found"
        for message in cursor.fetchall():
            cap = self.factory.capLookup(message[2])
            if cap == None:
                msg = "%s %s" % (message[2],message[3])
            else:
                msg = """(%s) %s: %s (%s) "%s": %s""" % (message[2],cap['group'],cap['city'],cap['region'],cap['name'],message[3])
            _return = "%s\n%s" %(_return, msg)
        return _return

class P2000BotFactory(protocol.ClientFactory):

    def __init__(self, channel, config):
        self.cfg = config
        self.channel = channel
        self.clients = []
        self.last_id = 0
        self.db = mdb.connect( 
            host=self.cfg.get('db','host'), user=self.cfg.get('db','user'),
            passwd=self.cfg.get('db','password'), db=self.cfg.get('db','database'))
        self.lc = task.LoopingCall(self.databaserunner)
        self.lc.start(2)

    def buildProtocol(self, addr):
        p = P2000Bot()
        p.factory = self
        return p

    def clientConnectionMade(self, client):
        self.clients.append(client)

    def clientConnectionLost(self, connector, reason):
        self.clients.remove(connector)
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        print "connection failed:", reason
        reactor.stop()

    def say(self,message):
        for client in self.clients:
            client.say(self.channel,"%s"%message)

    def capLookup(self,capid):
        cursor = self.db.cursor(mdb.cursors.DictCursor)
        cursor.execute("SELECT * FROM capcodes WHERE capid = %s LIMIT 1", capid)
        return cursor.fetchone()

   
    def databaserunner(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT lastid FROM queue LIMIT 1")
        last_id = cursor.fetchone()
        print "last id in the table: %s" % last_id
        cursor = self.db.cursor()
        cursor.execute("SELECT message_id,timestamp,cap,message FROM messages WHERE message_id > '%s' ORDER BY  `messages`.`timestamp` ASC LIMIT 5" % (last_id))
        for message in cursor.fetchall():
            for client in self.clients:
                cap = None
                if message[2] != None:
                    cap = self.capLookup(message[2])
                if cap == None:
                    msg = "%s %s" % (message[2],message[3])
                else:
                    msg = """(%s) %s: %s (%s) "%s": %s""" % (message[2],cap['group'],cap['city'],cap['region'],cap['name'],message[3])
                client.say(self.channel,msg)
                last_id = "%i" % message[0]
                print msg
        cursor = self.db.cursor()
        cursor.execute("UPDATE queue SET lastid = %s WHERE id = 0" % (last_id))
        self.db.commit()

if __name__ == '__main__':
    config = ConfigParser.RawConfigParser()
    config.read('bot.conf')
    log.startLogging(sys.stdout)
    f = P2000BotFactory(sys.argv[1],config)
    reactor.connectTCP("irc.smurfnet.ch", 6667, f)
    reactor.run()
