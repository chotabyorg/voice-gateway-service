"""Optional native speech classifier; no provider calls, text or recordings."""

from voice_gateway.media.audio import PCM, validate_pcm


class WebRTCSpeech:
    def __init__(self, mode=2):
        import webrtcvad

        self.detector = webrtcvad.Vad(mode)

    def __call__(self, frame):
        validate_pcm(frame)
        if not frame:
            return False
        if len(frame) > PCM.frame_bytes:
            raise ValueError("Speech classification requires at most 20 ms PCM")
        # Only shutdown can supply a partial frame; no sample is dropped.
        return self.detector.is_speech(frame.ljust(PCM.frame_bytes, b"\0"), PCM.sample_rate)
