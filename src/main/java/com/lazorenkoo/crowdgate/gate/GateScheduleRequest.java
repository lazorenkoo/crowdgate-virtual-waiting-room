package com.lazorenkoo.crowdgate.gate;

import java.time.Instant;

public record GateScheduleRequest(Long version, Instant opensAt, Instant closesAt) {
}
