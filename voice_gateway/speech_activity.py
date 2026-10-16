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


class TenSpeech:
    """Per-call TEN state; 8 kHz PCM is interpolated only for classification."""

    def __init__(self):
        import numpy as np
        from ten_vad import TenVad

        self.np = np
        self.detector = TenVad(hop_size=160, threshold=0.5)
        self.previous = 0

    def __call__(self, frame):
        validate_pcm(frame)
        if not frame:
            return False
        if len(frame) > PCM.frame_bytes:
            raise ValueError("Speech classification requires at most 20 ms PCM")
        np = self.np
        # A continuous, causal 2x linear interpolator. Use int32 for sums so
        # loud samples cannot wrap. The original 8 kHz bytes still reach STT.
        source = np.frombuffer(frame.ljust(PCM.frame_bytes, b"\0"), dtype="<i2").astype(np.int32)
        upsampled = np.empty(320, dtype=np.int16)
        upsampled[0] = (self.previous + int(source[0])) // 2
        upsampled[2::2] = (source[:-1] + source[1:]) // 2
        upsampled[1::2] = source
        self.previous = int(source[-1])
        # Process BOTH 10 ms hops even when the first is speech: native state
        # must see every sample, including silence and noise adaptation.
        first = self.detector.process(upsampled[:160])[1]
        second = self.detector.process(upsampled[160:])[1]
        return bool(first or second)
