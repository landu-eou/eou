#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, Optional, Tuple


BODY_HEAD_MAX = 300


def _ua(repo: str) -> str:
    return f"EOU-RQ-Gate-Kills/1.2 (+{repo}; GitHub Actions)"


def _truncate_text(text: str, limit: int = BODY_HEAD_MAX) -> str:
    if len(text) <= limit:
        return text.replace("\n", "\\n").replace("\r", "\\r")
    return text[:limit].replace("\n", "\\n").replace("\r", "\\r") + "...<truncated>"


def http_get_json(
    url: str,
    timeout: int,
    user_agent: str,
) -> Tuple[int, Dict[str, str], Optional[dict], str, str, str]:
    """
    Returns:
      status, headers, parsed_json_or_none, body_head, content_type, parse_state

    parse_state:
      - json_dict
      - json_non_dict
      - invalid_json
      - empty_body
      - http_error_no_json
      - exception
    """
    req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    try:
        with opener.open(req, timeout=timeout) as resp:
            status = resp.getcode()
            headers = {k: v for k, v in resp.headers.items()}
            raw = resp.read().decode("utf-8", errors="replace")
            body_head = _truncate_text(raw)
            content_type = headers.get("Content-Type", "")

            if not raw:
                return status, headers, None, body_head, content_type, "empty_body"

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                return status, headers, None, body_head, content_type, "invalid_json"

            if isinstance(data, dict):
                return status, headers, data, body_head, content_type, "json_dict"

            return status, headers, None, body_head, content_type, "json_non_dict"

    except urllib.error.HTTPError as e:
        headers = {k: v for k, v in e.headers.items()}
        raw = e.read().decode("utf-8", errors="replace")
        body_head = _truncate_text(raw)
        content_type = headers.get("Content-Type", "")

        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    return e.code, headers, data, body_head, content_type, "json_dict"
                return e.code, headers, None, body_head, content_type, "json_non_dict"
            except json.JSONDecodeError:
                return e.code, headers, None, body_head, content_type, "http_error_no_json"

        return e.code, headers, None, body_head, content_type, "empty_body"

    except Exception as ex:
        return 0, {}, None, _truncate_text(repr(ex)), "", "exception"


def write_outputs(kv: Dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def _run(cmd: list[str]) -> Tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr)


def git_flush(raw_path: str, msg: str) -> None:
    rc, _ = _run(["git", "diff", "--quiet", "--", raw_path])
    if rc == 0:
        return

    _run(["git", "add", raw_path])
    rc, out = _run(["git", "commit", "-m", msg])
    if rc != 0 and "nothing to commit" in out.lower():
        return

    rc, out = _run(["git", "push"])
    if rc == 0:
        return

    _run(["git", "pull", "--rebase"])
    rc, out = _run(["git", "push"])
    if rc != 0:
        print(f"[poll][git] push failed after retry: {out[:300]}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--raw", required=True)
    p.add_argument("--workflow-file", required=True)
    p.add_argument("--repo", required=True, help="owner/repo")
    p.add_argument("--queue-suffix", default="")
    p.add_argument("--ttw", type=int, default=1)
    p.add_argument("--poll-max-seconds", type=int, default=1200)
    p.add_argument("--null-max", type=int, default=10)  # reservado por compatibilidad CLI
    p.add_argument("--flush-seconds", type=int, default=60)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--min-interval", type=float, default=1.0)
    p.add_argument("--busy-429-max", type=int, default=8, help="si 429 consecutivos >= N => redisq_busy y se corta")
    p.add_argument("--debug-limit", type=int, default=20, help="máximo de logs detallados para respuestas anómalas")
    return p.parse_args()


def minimal_raw_line(pkg: dict) -> Optional[dict]:
    if not isinstance(pkg, dict):
        return None

    kill_id = pkg.get("killID")
    zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
    km_hash = zkb.get("hash")
    loc = zkb.get("locationID")
    labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

    if not isinstance(kill_id, int):
        return None
    if not isinstance(km_hash, str) or not km_hash:
        return None
    if not isinstance(loc, int):
        return None

    labels_out: list[str] = []
    for x in labels:
        if isinstance(x, str):
            labels_out.append(x)

    return {
        "killID": kill_id,
        "zkb": {
            "hash": km_hash,
            "locationID": loc,
            "labels": labels_out,
        },
        "rearm": 0,
    }


def _header(headers: Dict[str, str], key: str) -> str:
    for k, v in headers.items():
        if k.lower() == key.lower():
            return str(v)
    return ""


def _log_anomaly(
    *,
    idx: int,
    status: int,
    parse_state: str,
    content_type: str,
    body_head: str,
    headers: Dict[str, str],
    category: str,
) -> None:
    retry_after = _header(headers, "Retry-After")
    location = _header(headers, "Location")
    server = _header(headers, "Server")
    cf_ray = _header(headers, "CF-RAY")

    print(
        "[poll][anomaly] "
        f"n={idx} "
        f"category={category} "
        f"status={status} "
        f"parse_state={parse_state} "
        f"content_type={content_type or '-'} "
        f"retry_after={retry_after or '-'} "
        f"location={location or '-'} "
        f"server={server or '-'} "
        f"cf_ray={cf_ray or '-'} "
        f"body_head={body_head or '-'}"
    )


def main() -> int:
    args = parse_args()

    os.makedirs(os.path.dirname(args.raw), exist_ok=True)
    if not os.path.exists(args.raw):
        open(args.raw, "a", encoding="utf-8").close()

    owner_repo = args.repo
    if "/" not in owner_repo:
        print("ERROR: --repo must be owner/repo", file=sys.stderr)
        return 2

    owner, repo = owner_repo.split("/", 1)
    wf_base = args.workflow_file[:-4] if args.workflow_file.endswith(".yml") else args.workflow_file
    queue_id = f"{owner}/{repo}/{wf_base}" + (f"/{args.queue_suffix}" if args.queue_suffix else "")

    start = time.monotonic()
    last_req = 0.0

    # Mantengo tu lógica actual.
    empty_counter = 50

    received = 0
    accepted = 0
    discarded = 0
    flushes = 0

    http_429 = 0
    http_5xx = 0
    http_other = 0
    http_429_streak = 0
    http_429_streak_max = 0

    # Instrumentación nueva
    http_4xx_other = 0
    http_200_invalid_json = 0
    http_200_non_dict_json = 0
    http_200_missing_package = 0
    http_network_error = 0
    http_empty_body = 0
    http_redirect_like = 0
    debug_logs_emitted = 0
    last_error_status = 0
    last_error_parse_state = ""
    last_error_category = ""
    last_error_body_head = ""
    last_error_content_type = ""

    buffer: list[dict] = []
    poll_reason = "timeout"

    backoff = 0.0
    backoff_cap = 30.0

    def clamp_counter(x: int) -> int:
        if x < 0:
            return 0
        if x > 50:
            return 50
        return x

    def do_flush(label: str) -> None:
        nonlocal flushes
        if not buffer:
            return
        with open(args.raw, "a", encoding="utf-8") as f:
            for obj in buffer:
                f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        n = len(buffer)
        buffer.clear()
        flushes += 1
        print(f"[poll][flush] label={label} rows={n} total_flushes={flushes}")
        git_flush(args.raw, f"rq: poll flush (+{n})")

    def mark_anomaly(category: str, status: int, parse_state: str, body_head: str, content_type: str, headers: Dict[str, str]) -> None:
        nonlocal http_other, debug_logs_emitted
        nonlocal last_error_status, last_error_parse_state, last_error_category
        nonlocal last_error_body_head, last_error_content_type

        http_other += 1
        last_error_status = status
        last_error_parse_state = parse_state
        last_error_category = category
        last_error_body_head = body_head
        last_error_content_type = content_type

        if debug_logs_emitted < args.debug_limit:
            _log_anomaly(
                idx=debug_logs_emitted + 1,
                status=status,
                parse_state=parse_state,
                content_type=content_type,
                body_head=body_head,
                headers=headers,
                category=category,
            )
            debug_logs_emitted += 1

    try:
        window_end = start + args.flush_seconds

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= args.poll_max_seconds:
                poll_reason = "timeout"
                break

            if http_other >= 40:
                poll_reason = "error"
                break

            if http_429_streak >= args.busy_429_max:
                poll_reason = "redisq_busy"
                break

            now = time.monotonic()
            if now >= window_end:
                do_flush("window_end")
                window_end += args.flush_seconds

            since = time.monotonic() - last_req
            if since < args.min_interval:
                time.sleep(args.min_interval - since)

            url = args.base + "?" + urllib.parse.urlencode({"queueID": queue_id, "ttw": str(args.ttw)})
            status, headers, data, body_head, content_type, parse_state = http_get_json(
                url,
                timeout=args.timeout,
                user_agent=_ua(args.repo),
            )
            last_req = time.monotonic()

            if status == 200:
                http_429_streak = 0

                if parse_state == "json_dict" and isinstance(data, dict):
                    if "package" not in data:
                        http_200_missing_package += 1
                        mark_anomaly("200_missing_package", status, parse_state, body_head, content_type, headers)
                        backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                        time.sleep(backoff + random.uniform(0.0, 1.0))
                        continue

                    pkg = data.get("package")

                    if pkg is None:
                        empty_counter = clamp_counter(empty_counter - 1)
                    else:
                        empty_counter = clamp_counter(empty_counter + 1)

                    print(f"[poll] empty_counter={empty_counter}")

                    if empty_counter == 0:
                        poll_reason = "null_streak"
                        break

                    if isinstance(pkg, dict):
                        received += 1

                        zkb = pkg.get("zkb") if isinstance(pkg.get("zkb"), dict) else {}
                        location_id = zkb.get("locationID")
                        labels = zkb.get("labels") if isinstance(zkb.get("labels"), list) else []

                        pass_loc = isinstance(location_id, int) and (50000000 <= location_id <= 60000000)
                        pass_pvp = any(isinstance(x, str) and x == "pvp" for x in labels)
                        pass_cat6 = any(isinstance(x, str) and x == "cat:6" for x in labels)

                        print(
                            "[poll][kill] "
                            f"killID={pkg.get('killID')} "
                            f"locationID={location_id} "
                            f"pass_loc={pass_loc} "
                            f"pass_pvp={pass_pvp} "
                            f"pass_cat6={pass_cat6}"
                        )

                        if pass_loc and pass_pvp and pass_cat6:
                            line = minimal_raw_line(pkg)
                            if line is None:
                                discarded += 1
                                print(f"[poll][discard] reason=minimal_raw_line_none killID={pkg.get('killID')}")
                            else:
                                buffer.append(line)
                                accepted += 1
                                print(f"[poll][accept] killID={pkg.get('killID')} buffered={len(buffer)}")
                        else:
                            discarded += 1
                            print(f"[poll][discard] reason=filter1 killID={pkg.get('killID')}")

                        backoff = 0.0

                    elif pkg is None:
                        backoff = 0.0
                    else:
                        http_200_non_dict_json += 1
                        mark_anomaly("200_package_not_dict_or_null", status, parse_state, body_head, content_type, headers)
                        backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                        time.sleep(backoff + random.uniform(0.0, 1.0))

                elif parse_state == "invalid_json":
                    http_200_invalid_json += 1
                    mark_anomaly("200_invalid_json", status, parse_state, body_head, content_type, headers)
                    backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                    time.sleep(backoff + random.uniform(0.0, 1.0))

                elif parse_state == "json_non_dict":
                    http_200_non_dict_json += 1
                    mark_anomaly("200_json_non_dict", status, parse_state, body_head, content_type, headers)
                    backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                    time.sleep(backoff + random.uniform(0.0, 1.0))

                elif parse_state == "empty_body":
                    http_empty_body += 1
                    mark_anomaly("200_empty_body", status, parse_state, body_head, content_type, headers)
                    backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                    time.sleep(backoff + random.uniform(0.0, 1.0))

                else:
                    mark_anomaly("200_unexpected_parse_state", status, parse_state, body_head, content_type, headers)
                    backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                    time.sleep(backoff + random.uniform(0.0, 1.0))

            elif status == 429:
                http_429 += 1
                http_429_streak += 1
                http_429_streak_max = max(http_429_streak_max, http_429_streak)

                ra = _header(headers, "Retry-After")
                base_sleep = int(ra) if (ra and ra.isdigit()) else 5
                sleep_s = base_sleep + random.uniform(0.0, 1.5)

                if debug_logs_emitted < args.debug_limit:
                    _log_anomaly(
                        idx=debug_logs_emitted + 1,
                        status=status,
                        parse_state=parse_state,
                        content_type=content_type,
                        body_head=body_head,
                        headers=headers,
                        category="429",
                    )
                    debug_logs_emitted += 1

                print(f"[poll][429] streak={http_429_streak} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

            elif status in (500, 502, 503, 504):
                http_5xx += 1
                http_429_streak = 0
                backoff = min(backoff_cap, backoff * 2.0 if backoff > 0.0 else 5.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)

                if debug_logs_emitted < args.debug_limit:
                    _log_anomaly(
                        idx=debug_logs_emitted + 1,
                        status=status,
                        parse_state=parse_state,
                        content_type=content_type,
                        body_head=body_head,
                        headers=headers,
                        category="5xx",
                    )
                    debug_logs_emitted += 1

                print(f"[poll][5xx] status={status} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

            elif status == 0:
                http_network_error += 1
                http_429_streak = 0
                backoff = min(backoff_cap, backoff * 2.0 if backoff > 0.0 else 5.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                mark_anomaly("network_or_exception", status, parse_state, body_head, content_type, headers)
                print(f"[poll][network] sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

            elif status in (301, 302, 303, 307, 308):
                http_redirect_like += 1
                http_429_streak = 0
                mark_anomaly("unexpected_redirect_status", status, parse_state, body_head, content_type, headers)
                backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                print(f"[poll][redirect-like] status={status} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

            elif 400 <= status < 500:
                http_4xx_other += 1
                http_429_streak = 0
                mark_anomaly("4xx_other", status, parse_state, body_head, content_type, headers)
                backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                print(f"[poll][4xx] status={status} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

            else:
                http_429_streak = 0
                mark_anomaly("unexpected_status", status, parse_state, body_head, content_type, headers)
                backoff = min(backoff_cap, backoff * 1.5 if backoff > 0.0 else 3.0)
                sleep_s = backoff + random.uniform(0.0, 1.0)
                print(f"[poll][unexpected] status={status} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

        do_flush("final")

    finally:
        pass

    duration = int(time.monotonic() - start)

    print(
        "[poll][summary] "
        f"poll_reason={poll_reason} "
        f"duration_seconds={duration} "
        f"queue_id={queue_id} "
        f"received={received} accepted={accepted} discarded={discarded} "
        f"empty_counter={empty_counter} flushes={flushes} "
        f"http_429={http_429} http_429_streak_max={http_429_streak_max} "
        f"http_5xx={http_5xx} http_other={http_other} "
        f"http_4xx_other={http_4xx_other} "
        f"http_200_invalid_json={http_200_invalid_json} "
        f"http_200_non_dict_json={http_200_non_dict_json} "
        f"http_200_missing_package={http_200_missing_package} "
        f"http_network_error={http_network_error} "
        f"http_empty_body={http_empty_body} "
        f"http_redirect_like={http_redirect_like} "
        f"last_error_category={last_error_category or '-'} "
        f"last_error_status={last_error_status} "
        f"last_error_parse_state={last_error_parse_state or '-'} "
        f"last_error_content_type={last_error_content_type or '-'} "
        f"last_error_body_head={last_error_body_head or '-'}"
    )

    write_outputs(
        {
            "poll_reason": poll_reason,
            "queue_id": queue_id,
            "duration_seconds": duration,
            "received": received,
            "accepted": accepted,
            "discarded": discarded,
            "null_streak": empty_counter,  # mantenido por compatibilidad con tu workflow
            "flushes": flushes,
            "http_429": http_429,
            "http_429_streak_max": http_429_streak_max,
            "http_5xx": http_5xx,
            "http_other": http_other,
            "http_4xx_other": http_4xx_other,
            "http_200_invalid_json": http_200_invalid_json,
            "http_200_non_dict_json": http_200_non_dict_json,
            "http_200_missing_package": http_200_missing_package,
            "http_network_error": http_network_error,
            "http_empty_body": http_empty_body,
            "http_redirect_like": http_redirect_like,
            "last_error_category": last_error_category,
            "last_error_status": last_error_status,
            "last_error_parse_state": last_error_parse_state,
            "last_error_content_type": last_error_content_type,
            "last_error_body_head": last_error_body_head,
        }
    )

    return 1 if poll_reason == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
