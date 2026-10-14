package com.lazorenkoo.crowdgate.gate;

import java.util.List;

public record GatePage(
        List<Gate> items,
        int page,
        int size,
        long totalItems,
        int totalPages
) {
}
