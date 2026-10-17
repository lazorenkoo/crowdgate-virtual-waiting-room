import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const base = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const gate = __ENV.GATE_ID;
const vus = integer('VUS', 2);
const iterations = integer('ITERATIONS', 10);
const polls = integer('POLL_ATTEMPTS', 8);
const joinLatency = new Trend('crowdgate_join_latency_ms', true);
const duplicates = new Rate('crowdgate_duplicate_entry');
const admitted = new Counter('crowdgate_observed_admissions');
const waiting = new Counter('crowdgate_still_waiting');

if (!gate || !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(gate)) {
    throw new Error('GATE_ID must be a canonical UUID');
}
if ((vus > 100 || iterations > 1000) && __ENV.ALLOW_LARGE_LOAD !== 'true') {
    throw new Error('Large runs require ALLOW_LARGE_LOAD=true');
}

export const options = {
    noCookiesReset: false,
    systemTags: ['status', 'method', 'name', 'scenario', 'expected_response'],
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
    scenarios: {
        queue: {
            executor: 'shared-iterations',
            vus,
            iterations,
            maxDuration: __ENV.MAX_DURATION || '2m',
        },
    },
    thresholds: {
        checks: ['rate>0.99'],
        http_req_failed: ['rate<0.01'],
        crowdgate_duplicate_entry: ['rate==0'],
        crowdgate_join_latency_ms: ['p(95)<500', 'p(99)<1000'],
    },
};

export default function () {
    const joined = http.post(`${base}/api/v1/gates/${gate}/join`, null,
        { tags: { name: 'POST gate join' }, timeout: '5s' });
    joinLatency.add(joined.timings.duration);
    let entry = body(joined);
    const valid = check(joined, {
        'join returns a queue entry': (response) => response.status === 200 && entry
            && typeof entry.queueToken === 'string'
            && entry.queueToken.startsWith(`${gate}:`)
            && ['WAITING', 'ADMITTED'].includes(entry.status),
    });
    if (!valid) {
        return;
    }

    const token = entry.queueToken;
    const repeated = http.post(`${base}/api/v1/gates/${gate}/join`, null,
        { tags: { name: 'POST repeated join' }, timeout: '5s' });
    const duplicate = body(repeated);
    check(repeated, {
        'repeated join keeps the same entry': (response) => response.status === 200
            && duplicate && duplicate.queueToken === token,
    });
    duplicates.add(Boolean(duplicate && duplicate.queueToken && duplicate.queueToken !== token));

    for (let attempt = 0; attempt < polls && entry.status === 'WAITING'; attempt++) {
        sleep(0.25);
        const response = http.get(`${base}/api/v1/queue/${token}`,
            { tags: { name: 'GET queue status' }, timeout: '5s' });
        entry = body(response);
        if (!check(response, {
            'status belongs to this entry': (value) => value.status === 200 && entry
                && entry.queueToken === token && ['WAITING', 'ADMITTED'].includes(entry.status),
        })) {
            return;
        }
    }
    if (entry.status === 'ADMITTED') {
        admitted.add(1);
    } else {
        waiting.add(1);
    }
}

function body(response) {
    try {
        return response.json();
    } catch (_) {
        return null;
    }
}

function integer(name, fallback) {
    const value = Number(__ENV[name] || fallback);
    if (!Number.isSafeInteger(value) || value < 1) {
        throw new Error(`${name} must be a positive integer`);
    }
    return value;
}
