import socket, argparse, sys, heapq, xml.etree.ElementTree as ET, os

def make_rw(sock):
    fin  = sock.makefile("r", encoding="utf-8", newline="\n")
    fout = sock.makefile("w", encoding="utf-8", newline="\n")
    return fin, fout

def send(fout, line, verbose=True):
    if verbose: print(">>", line)
    fout.write(line + "\n"); fout.flush()

def recv_line(fin, verbose=True):
    line = fin.readline()
    if not line:
        raise ConnectionError("Server closed")
    line = line.rstrip("\n")
    if verbose: print("<<", line)
    return line

def expect_ok(fin, verbose=True):
    if recv_line(fin, verbose) != "OK":
        raise RuntimeError("Expected OK")

def parse_server_row(line):
    f = line.split()
    return {
        "type": f[0],
        "id":   int(f[1]),
        "state": f[2],                 
        "curStartTime": int(f[3]),
        "cores": int(f[4]),            
        "mem":   int(f[5]),
        "disk":  int(f[6]),
        "wJobs": int(f[7]),
        "rJobs": int(f[8]),
    }

def gets_all(fin, fout, verbose=True):
    send(fout, "GETS All", verbose)
    header = recv_line(fin, verbose)           
    parts = header.split()
    if parts[0] != "DATA": raise RuntimeError(f"Unexpected header: {header}")
    n = int(parts[1])
    send(fout, "OK", verbose)
    rows = [recv_line(fin, verbose) for _ in range(n)]
    send(fout, "OK", verbose)
    if recv_line(fin, verbose) != ".": raise RuntimeError("Expected '.'")
    return [parse_server_row(r) for r in rows]

def gets_capable(fin, fout, c, m, d, verbose=True):
    send(fout, f"GETS Capable {c} {m} {d}", verbose)
    header = recv_line(fin, verbose)          
    parts = header.split()
    if parts[0] != "DATA": raise RuntimeError(f"Unexpected header for GETS Capable: {header}")
    n = int(parts[1])
    send(fout, "OK", verbose)
    rows = [recv_line(fin, verbose) for _ in range(n)]
    send(fout, "OK", verbose)
    if recv_line(fin, verbose) != ".": raise RuntimeError("Expected '.'")
    return [parse_server_row(r) for r in rows]

# prediction model (per-server)
class ServerSched:
    """Track predicted running cores via a min-heap of (end_time, cores_used)."""
    __slots__ = ("cores_total","heap")
    def __init__(self, cores_total):
        self.cores_total = cores_total
        self.heap = []  # (end_time, cores_used)
    def prune_to_time(self, now):
        while self.heap and self.heap[0][0] <= now:
            heapq.heappop(self.heap)

    def running_cores(self):
        return sum(c for _, c in self.heap)

    def earliest_start_for(self, now, c_need):
        self.prune_to_time(now)
        avail = self.cores_total - self.running_cores()
        if avail >= c_need:
            return now
        # simulate releases in time order
        tmp = sorted(self.heap)
        t = now
        avail_now = avail
        for end_t, c in tmp:
            if end_t > t:
                t = end_t
            avail_now += c
            if avail_now >= c_need:
                return t
        return t

    def add_job(self, start_t, c_need, est):
        end_t = start_t + est
        heapq.heappush(self.heap, (end_t, c_need))
        return end_t

# helpers
def read_system_info(path="ds-system.xml"):
    """Parse ds-system.xml -> maps for boot time, hourly rate, cores per type."""
    t_boot, t_rate, t_cores = {}, {}, {}
    if not os.path.exists(path):
        return t_boot, t_rate, t_cores  # can still run with defaults
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        for s in root.findall(".//server"):
            ty = s.attrib["type"]
            t_boot[ty]  = int(round(float(s.attrib["bootupTime"])))
            t_rate[ty]  = float(s.attrib["hourlyRate"])
            t_cores[ty] = int(s.attrib["cores"])
    except Exception:
        pass
    return t_boot, t_rate, t_cores

def state_penalty_seconds(state, boot_seconds):
    # ACTIVE: 0, BOOTING/INACTIVE: penalise by boot time; IDLE is immediately runnable
    if state == "active" or state == "idle":
        return 0
    return max(0, boot_seconds)

def main():
    ap = argparse.ArgumentParser(description="EFT+ (Earliest Finish with boot/cost awareness)")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=50000)
    ap.add_argument("--user", default="eftplus")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--cost_bias", type=float, default=0.0001,
                    help="Tie-breaker weight for hourlyRate (smaller is gentler)")
    ap.add_argument("--fallback_boot_penalty", type=int, default=80,
                    help="Used if ds-system.xml missing")
    args = ap.parse_args()
    verbose = not args.quiet

    # Connect
    sock = socket.socket()
    try:
        sock.connect((args.host, args.port))
    except OSError as e:
        print(f"Connect failed: {e}", file=sys.stderr); sys.exit(1)
    fin, fout = make_rw(sock)

    # Read ds-system.xml if present
    type_boot, type_rate, type_cores = read_system_info("ds-system.xml")

    # Keep a per-(type,id) predicted schedule model with *total* cores for that server type
    sched_by_srv = {}

    try:
        # Handshake
        send(fout, "HELO", verbose); expect_ok(fin, verbose)
        send(fout, f"AUTH {args.user}", verbose); expect_ok(fin, verbose)

        # Initialize models from GETS All (to know server ids and current avail cores, total cores from ds-system.xml)
        for s in gets_all(fin, fout, verbose):
            tot = type_cores.get(s["type"], s["cores"])  # if ds-system.xml absent, use reported as a proxy
            sched_by_srv[(s["type"], s["id"])] = ServerSched(tot)

        now = 0

        while True:
            send(fout, "REDY", verbose)
            line = recv_line(fin, verbose)
            parts = line.split()
            kind = parts[0]

            if kind == "JOBN":
                # JOBN id submit cores mem disk est
                job_id  = int(parts[1])
                now     = int(parts[2])
                j_cores = int(parts[3]); j_mem = int(parts[4]); j_disk = int(parts[5]); j_est = int(parts[6])

                caps = gets_capable(fin, fout, j_cores, j_mem, j_disk, verbose)
                if not caps:
                    # Shouldn't happen, but stay robust
                    caps = gets_all(fin, fout, verbose)

                # Fast path: immediate run on an ACTIVE server with no queue and enough available cores
                instant = [s for s in caps
                           if s["state"] in ("active", "idle") and s["wJobs"] == 0 and s["cores"] >= j_cores]
                if instant:
                    # Prefer smallest cores_total among instant candidates to curb cost
                    best = min(
                        ((type_cores.get(s["type"], s["cores"]), s["type"], s["id"]) for s in instant),
                        key=lambda x: (x[0], x[1], x[2])
                    )
                    stype, sid = best[1], best[2]
                    send(fout, f"SCHD {job_id} {stype} {sid}", verbose); expect_ok(fin, verbose)
                    # Predict immediately starting now
                    sched_by_srv[(stype, sid)].add_job(now, j_cores, j_est)
                    continue

                # General path: compute predicted start/finish with boot & cost bias
                best_tuple = None  # (finish, start, rate_bias, cores_total, type, id)
                for s in caps:
                    stype, sid = s["type"], s["id"]
                    sched = sched_by_srv.get((stype, sid))
                    if not sched:
                        sched = sched_by_srv[(stype, sid)] = ServerSched(type_cores.get(stype, s["cores"]))

                    # earliest start from our predicted timeline
                    start = sched.earliest_start_for(now, j_cores)

                    # add boot penalty if not already runnable (non-active / non-idle)
                    boot_sec = type_boot.get(stype, args.fallback_boot_penalty)
                    start += state_penalty_seconds(s["state"], boot_sec)

                    finish = start + j_est
                    rate   = type_rate.get(stype, 0.0)
                    rate_bias = args.cost_bias * rate * j_est  # tiny cost shaping

                    cand = (finish, start, rate_bias, type_cores.get(stype, s["cores"]), stype, sid)
                    if (best_tuple is None) or (cand < best_tuple):
                        best_tuple = cand

                _, chosen_start, _, _, stype, sid = best_tuple
                send(fout, f"SCHD {job_id} {stype} {sid}", verbose); expect_ok(fin, verbose)
                sched_by_srv[(stype, sid)].add_job(chosen_start, j_cores, j_est)

            elif kind == "JCPL":
                # JCPL t jobID serverType serverID
                now = int(parts[1])
                stype = parts[3]; sid = int(parts[4])
                # prune that server’s finished jobs up to 'now'
                key = (stype, sid)
                if key in sched_by_srv:
                    sched_by_srv[key].prune_to_time(now)
                continue

            elif kind == "NONE":
                send(fout, "QUIT", verbose)
                _ = recv_line(fin, verbose)  # QUIT / final ack
                break

            else:
                # RESF/RESR/etc: just ask for the next event
                continue

    finally:
        try: sock.close()
        except: pass

if __name__ == "__main__":
    main()
