"""Deterministic native PJSUA2 boundary double, adapted from donor SIP tests."""

from collections import deque
from types import SimpleNamespace as NS


def native():
    """Expose a configurable compiled ceiling through a deterministic PJSUA2 fake."""
    pj = NS(events=deque(), calls=[], endpoints=[], registration_code=200, capacity=64)
    pj.PJSUA_INVALID_ID = -1
    pj.PJSIP_TRANSPORT_UDP = 1
    pj.PJSIP_INV_STATE_CONFIRMED = 5
    pj.PJSIP_INV_STATE_DISCONNECTED = 6
    pj.PJMEDIA_TYPE_AUDIO = 1
    pj.PJMEDIA_FRAME_TYPE_AUDIO = 1
    pj.PJSUA_CALL_MEDIA_ACTIVE = 1

    class Endpoint:
        def __init__(self):
            pj.endpoints.append(self)
            self.codecs = []
            self.destroyed = False

        def libCreate(self):
            pass

        def libInit(self, conf):
            self.conf = conf

        def transportCreate(self, kind, conf):
            self.transport_conf = conf
            return 7

        def libStart(self):
            pass

        def audDevManager(self):
            return NS(setNullDev=lambda: None)

        def codecEnum2(self):
            return [NS(codecId=c) for c in ("PCMA/8000/1", "PCMU/8000/1", "opus/48000/2")]

        def codecSetPriority(self, name, priority):
            self.codecs.append((name, priority))

        def libHandleEvents(self, timeout):
            if pj.events:
                pj.events.popleft()()

        def libDestroy(self):
            self.destroyed = True

    class Account:
        def create(self, conf):
            self.conf = conf
            pj.events.append(lambda: self.onRegState(NS(code=pj.registration_code)))

        def getInfo(self):
            return NS(regIsActive=pj.registration_code == 200)

        def shutdown(self):
            self.stopped = True

    class Media:
        def __init__(self):
            self.targets = []

        def startTransmit(self, target):
            self.targets.append(target)

        def stopTransmit(self, target):
            if target in self.targets:
                self.targets.remove(target)

    class Port(Media):
        def createPort(self, name, fmt):
            self.format = fmt

    class Call:
        def __init__(self, account, call_id):
            self.account, self.call_id = account, call_id
            self.state = 0
            self.media_native = Media()
            self.media_active = False
            self.hangups = 0
            pj.calls.append(self)

        def getInfo(self):
            return NS(
                state=self.state,
                remoteUri='"Caller" <sip:375291234567@chota.pbx.bn.by>',
                media=[NS(type=1, status=1, index=0)] if self.media_active else [],
            )

        def getAudioMedia(self, index):
            return self.media_native

        def answer(self, options):
            self.answer_code = options.statusCode

        def makeCall(self, uri, options):
            self.destination = uri

        def hangup(self, options):
            self.hangups += 1
            pj.events.append(self.disconnect)

        def activate_media(self):
            self.media_active = True
            self.onCallMediaState(NS())

        def confirm(self):
            self.state = 5
            self.onCallState(NS())

        def disconnect(self):
            self.state = 6
            self.onCallState(NS())

    pj.Endpoint, pj.Account, pj.Call, pj.AudioMediaPort = Endpoint, Account, Call, Port
    pj.EpConfig = lambda: NS(uaConfig=NS(maxCalls=pj.capacity), medConfig=NS(), logConfig=NS())
    pj.TransportConfig = NS
    pj.AccountConfig = lambda: NS(
        regConfig=NS(),
        sipConfig=NS(proxies=[], authCreds=[]),
        natConfig=NS(),
        mediaConfig=NS(transportConfig=NS()),
    )
    pj.AuthCredInfo = lambda *args: args
    pj.CallOpParam = lambda *args: NS(opt=NS())
    pj.MediaFormatAudio = NS
    return pj
