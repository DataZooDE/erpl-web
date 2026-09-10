#!/usr/bin/env python3
"""Page-size histogram for the OData services we can actually reach (GitHub #89).

Reports bytes per page, rows per page, whether a next link follows, and whether the
service echoed `Preference-Applied` for a `Prefer: odata.maxpagesize` we sent.

MEASURED 2026-09-10 - the histogram issue #89 asked for before optimising
---------------------------------------------------------------------------
    service / entity set              bytes/page   rows/page   bytes/row
    services.odata.org V2 Orders         203_569         200        1018
    services.odata.org V2 Customers       14_825          20         741
    services.odata.org V2 Order_Details  307_745         500         615
    services.odata.org V4 Orders          77_606         200         388
    services.odata.org V4 Customers        5_565          20         278
    services.odata.org V4 Order_Details   40_433         500          81
    TripPin People                         7_101          20         355
    TripPin Airports                       1_889           5         378
    V2 Customers?$expand=Orders          196_465          20        9823
    V4 Customers?$expand=Orders           75_227          20        3761

Every one of those services caps the page itself and keeps capping it under
`$top=800` / `$top=3000`. The largest page any reachable service produced is
308 KB. `$expand` widens rows but the row cap still bounds the page.

`Prefer: odata.maxpagesize` was sent to all of them and NONE honoured it: no
`Preference-Applied` header came back and the page sizes were byte-identical to the
unhinted requests. It is a hint, and the reference implementations ignore it.

The Microsoft Graph tenant available to the tests is too small to exercise Graph's
own caps (3 users, 7 groups, 2 messages, 2 list items - pages of 1-10 KB), so Graph
contributes a lower bound here, not a cap measurement.

USAGE
-----
    test/perf/odata_page_sizes.py                 # the public services above
    test/perf/odata_page_sizes.py --prefer 50     # re-check maxpagesize honouring
"""
import argparse
import json
import sys
import urllib.request

PUBLIC_ENDPOINTS = [
    ("nw-v2 Orders", "https://services.odata.org/V2/Northwind/Northwind.svc/Orders?$format=json"),
    ("nw-v2 Customers", "https://services.odata.org/V2/Northwind/Northwind.svc/Customers?$format=json"),
    ("nw-v2 Order_Details", "https://services.odata.org/V2/Northwind/Northwind.svc/Order_Details?$format=json"),
    ("nw-v2 Customers+expand", "https://services.odata.org/V2/Northwind/Northwind.svc/Customers?$format=json&$expand=Orders"),
    ("nw-v4 Orders", "https://services.odata.org/V4/Northwind/Northwind.svc/Orders"),
    ("nw-v4 Customers", "https://services.odata.org/V4/Northwind/Northwind.svc/Customers"),
    ("nw-v4 Order_Details", "https://services.odata.org/V4/Northwind/Northwind.svc/Order_Details"),
    ("nw-v4 Customers+expand", "https://services.odata.org/V4/Northwind/Northwind.svc/Customers?$expand=Orders"),
    ("nw-v4 Orders top=800", "https://services.odata.org/V4/Northwind/Northwind.svc/Orders?$top=800"),
    ("trippin People", "https://services.odata.org/TripPinRESTierService/People"),
    ("trippin Airports", "https://services.odata.org/TripPinRESTierService/Airports"),
]


def row_count(document):
    """Row count for either shape: v4 `value`, v2 `d.results`, or a bare `d` array."""
    if isinstance(document.get("value"), list):
        return len(document["value"])
    wrapper = document.get("d")
    if isinstance(wrapper, dict) and isinstance(wrapper.get("results"), list):
        return len(wrapper["results"])
    if isinstance(wrapper, list):
        return len(wrapper)
    return None


def next_link(document):
    if "@odata.nextLink" in document:
        return document["@odata.nextLink"]
    wrapper = document.get("d")
    if isinstance(wrapper, dict):
        return wrapper.get("__next")
    return None


def probe(label, url, prefer=None, bearer=None):
    request = urllib.request.Request(url)
    request.add_header("Accept", "application/json")
    if prefer is not None:
        request.add_header("Prefer", f"odata.maxpagesize={prefer}")
    if bearer:
        request.add_header("Authorization", f"Bearer {bearer}")
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            body = response.read()
            applied = dict(response.headers).get("Preference-Applied", "-")
    except Exception as error:  # noqa: BLE001
        print(f"{label}\tERROR {error}")
        return

    try:
        document = json.loads(body)
    except ValueError:
        print(f"{label}\tbytes={len(body)}\tNON-JSON")
        return

    rows = row_count(document)
    per_row = (len(body) / rows) if rows else 0
    print(f"{label}\tbytes={len(body)}\trows={rows}\tbytes/row={per_row:.0f}\t"
          f"next={'yes' if next_link(document) else 'no'}\tPreference-Applied={applied}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefer", type=int, default=None,
                        help="send Prefer: odata.maxpagesize=N and report what came back")
    parser.add_argument("--url", action="append", default=[],
                        help="probe an extra URL instead of relying on the built-in list")
    parser.add_argument("--bearer", default=None, help="bearer token for authenticated services")
    args = parser.parse_args()

    endpoints = [(url, url) for url in args.url] or PUBLIC_ENDPOINTS
    for label, url in endpoints:
        probe(label, url, args.prefer, args.bearer)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
