"""
Seed Metabase with Product View Analytics dashboard.

Adds 6 questions to the existing 'project product' dashboard:
- Views by date
- Top products by views
- Top stores by views
- Referrer breakdown
- Device traffic
- Product x store matrix

Usage:
    set METABASE_URL=http://localhost:3000
    set METABASE_USER=...
    set METABASE_PASSWORD=...
    python metabase/seed_dashboard.py
"""
import json
import os
import sys
import urllib.error
import urllib.request

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
METABASE_USER = os.getenv("METABASE_USER", "")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "")
DASHBOARD_NAME = os.getenv("METABASE_DASHBOARD", "project product")
DATABASE_NAME = os.getenv("METABASE_DATABASE", "Product View Warehouse")
COLLECTION_NAME = os.getenv("METABASE_COLLECTION", "Product View Analytics")


def request(path, method="GET", data=None, token=None):
    url = f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.loads(response.read().decode("utf-8") or "null")
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: {error.code} {payload}") from error


def login():
    if not METABASE_USER or not METABASE_PASSWORD:
        print("Set METABASE_USER and METABASE_PASSWORD environment variables first.")
        sys.exit(1)
    status, body = request("/api/session", method="POST", data={
        "username": METABASE_USER,
        "password": METABASE_PASSWORD,
    })
    if status != 200 or "id" not in body:
        print("Login failed:", body)
        sys.exit(1)
    return body["id"]


def find_database(token):
    status, body = request("/api/database", token=token)
    for database in body.get("data", []):
        if database.get("name") == DATABASE_NAME:
            return database
    if body.get("data"):
        return body["data"][0]
    print("No database configured in Metabase. Add the PostgreSQL connection first.")
    sys.exit(1)


def find_or_create_collection(token):
    status, body = request("/api/collection", token=token)
    for collection in body or []:
        if collection.get("name") == COLLECTION_NAME and not collection.get("archived"):
            print(f"Using collection id={collection['id']} name='{collection['name']}'")
            return collection
    payload = {"name": COLLECTION_NAME, "description": "Product View Analytics questions and dashboard"}
    status, created = request("/api/collection", method="POST", data=payload, token=token)
    if status not in (200, 201):
        print("Failed to create collection:", created)
        sys.exit(1)
    print(f"Created collection id={created['id']} name='{created['name']}'")
    return created


def find_or_create_dashboard(token, question_ids, collection):
    status, body = request("/api/dashboard", token=token)
    for dashboard in body or []:
        if dashboard.get("name") == DASHBOARD_NAME:
            if str(dashboard.get("collection_id")) != str(collection["id"]):
                print(f"Moving dashboard id={dashboard['id']} into collection id={collection['id']}")
                request(
                    f"/api/dashboard/{dashboard['id']}",
                    method="PUT",
                    data={"collection_id": collection["id"]},
                    token=token,
                )
            print(f"Existing dashboard id={dashboard['id']} - rebuilding layout")
            return dashboard

    payload = {"name": DASHBOARD_NAME, "description": "Product View Analytics", "collection_id": collection["id"]}
    status, created = request("/api/dashboard", method="POST", data=payload, token=token)
    if status not in (200, 201):
        print("Failed to create dashboard:", created)
        sys.exit(1)
    print(f"Created dashboard id={created['id']}")
    return created


def attach_cards(token, dashboard, card_ids):
    layout = [
        (0, 0, 12, 6),
        (12, 0, 12, 6),
        (0, 6, 12, 6),
        (12, 6, 12, 6),
        (0, 12, 24, 6),
        (0, 18, 24, 6),
    ]
    dashcards = [
        {
            "id": -(index + 1),
            "card_id": card_id,
            "row": col_row[1],
            "col": col_row[0],
            "size_x": col_row[2],
            "size_y": col_row[3],
        }
        for index, (card_id, col_row) in enumerate(zip(card_ids, layout))
    ]
    payload = {"dashcards": dashcards, "tabs": []}
    status, _ = request(
        f"/api/dashboard/{dashboard['id']}",
        method="PUT",
        data=payload,
        token=token,
    )
    print(f"Attached {len(dashcards)} cards to dashboard id={dashboard['id']}")


def build_questions(database_id):
    line_settings = {
        "graph.dimensions": ["date"],
        "graph.metrics": ["total_views", "unique_visitors"],
        "graph.show_values": True,
        "graph.x_axis.labels_enabled": True,
    }
    bar_settings = {
        "graph.dimensions": ["name"],
        "graph.metrics": ["total_views"],
        "graph.show_values": True,
    }
    pie_settings = {
        "pie.dimension": "type",
        "pie.metric": "total_views",
        "show_legend": True,
    }
    matrix_settings = {
        "table.pivot_column": "store_id",
        "table.cell_column": "total_views",
    }

    return [
        {
            "name": "Views by date",
            "display": "line",
            "visualization_settings": line_settings,
            "sql": """
                SELECT
                  dd.date AS date,
                  SUM(fpv.view_count) AS total_views,
                  SUM(fpv.unique_visitors) AS unique_visitors
                FROM public.fact_product_views fpv
                JOIN public.dim_date dd ON fpv.date_key = dd.date_key
                GROUP BY dd.date
                ORDER BY dd.date
            """,
        },
        {
            "name": "Top products by views",
            "display": "bar",
            "visualization_settings": bar_settings,
            "sql": """
                SELECT
                  dp.product_name AS name,
                  dp.product_id,
                  SUM(fpv.view_count) AS total_views,
                  SUM(fpv.unique_visitors) AS unique_visitors
                FROM public.fact_product_views fpv
                JOIN public.dim_product dp ON fpv.product_key = dp.product_key
                GROUP BY dp.product_name, dp.product_id
                ORDER BY total_views DESC
                LIMIT 20
            """,
        },
        {
            "name": "Top stores by views",
            "display": "bar",
            "visualization_settings": bar_settings,
            "sql": """
                SELECT
                  dc.country_name AS name,
                  dc.store_id,
                  SUM(fpv.view_count) AS total_views,
                  SUM(fpv.unique_visitors) AS unique_visitors
                FROM public.fact_product_views fpv
                LEFT JOIN public.dim_country dc ON fpv.country_key = dc.country_key
                GROUP BY dc.country_name, dc.store_id
                ORDER BY total_views DESC
                LIMIT 20
            """,
        },
        {
            "name": "Referrer breakdown",
            "display": "pie",
            "visualization_settings": pie_settings,
            "sql": """
                SELECT
                  COALESCE(dr.referrer_type, 'Unknown') AS type,
                  SUM(fpv.view_count) AS total_views,
                  SUM(fpv.unique_visitors) AS unique_visitors
                FROM public.fact_product_views fpv
                LEFT JOIN public.dim_referrer dr ON fpv.referrer_key = dr.referrer_key
                GROUP BY COALESCE(dr.referrer_type, 'Unknown')
                ORDER BY total_views DESC
            """,
        },
        {
            "name": "Device traffic",
            "display": "pie",
            "visualization_settings": pie_settings,
            "sql": """
                SELECT
                  COALESCE(ddv.device_type, 'Unknown') AS type,
                  SUM(fpv.view_count) AS total_views,
                  SUM(fpv.unique_visitors) AS unique_visitors
                FROM public.fact_product_views fpv
                LEFT JOIN public.dim_device ddv ON fpv.device_key = ddv.device_key
                GROUP BY COALESCE(ddv.device_type, 'Unknown')
                ORDER BY total_views DESC
            """,
        },
        {
            "name": "Product x store matrix",
            "display": "table",
            "visualization_settings": matrix_settings,
            "sql": """
                SELECT
                  dp.product_id,
                  dc.store_id,
                  SUM(fpv.view_count) AS total_views
                FROM public.fact_product_views fpv
                JOIN public.dim_product dp ON fpv.product_key = dp.product_key
                LEFT JOIN public.dim_country dc ON fpv.country_key = dc.country_key
                GROUP BY dp.product_id, dc.store_id
                ORDER BY total_views DESC
                LIMIT 50
            """,
        },
    ]


def create_card(token, database_id, question, collection_id):
    payload = {
        "name": question["name"],
        "display": question["display"],
        "visualization_settings": question["visualization_settings"],
        "dataset_query": {
            "type": "native",
            "native": {"query": question["sql"]},
            "database": database_id,
        },
        "collection_id": collection_id,
    }
    status, body = request("/api/card", method="POST", data=payload, token=token)
    if status not in (200, 201):
        print(f"Failed to create card '{question['name']}': {body}")
        sys.exit(1)
    return body["id"]


def arrange_dashcards(existing_cards, dashcards):
    layout = [
        (0, 0, 12, 6),
        (12, 0, 12, 6),
        (0, 6, 12, 6),
        (12, 6, 12, 6),
        (0, 12, 24, 6),
        (0, 18, 24, 6),
    ]
    next_row = 24
    updates = []
    for index, dashcard in enumerate(dashcards):
        if index < len(layout):
            col, row, size_x, size_y = layout[index]
        else:
            col, row, size_x, size_y = 0, next_row, 24, 6
            next_row += size_y
        dashcard["col"] = col
        dashcard["row"] = row
        dashcard["size_x"] = size_x
        dashcard["size_y"] = size_y
        updates.append(dashcard)
    next(existing_cards, None)
    return updates


def archive_duplicate_cards(token, question_names):
    status, body = request("/api/card", token=token)
    for card in body or []:
        if card.get("name") in question_names and not card.get("archived"):
            print(f"  archive existing card id={card['id']} name='{card['name']}'")
            request(f"/api/card/{card['id']}", method="DELETE", token=token)


def main():
    print(f"Connecting to Metabase at {METABASE_URL} ...")
    token = login()
    print("Login OK.")

    database = find_database(token)
    print(f"Using database id={database['id']} name={database['name']}")

    collection = find_or_create_collection(token)
    questions = build_questions(database["id"])
    archive_duplicate_cards(token, {q["name"] for q in questions})

    card_ids = []
    for question in questions:
        card_id = create_card(token, database["id"], question, collection["id"])
        print(f"  card id={card_id} name='{question['name']}'")
        card_ids.append(card_id)

    dashboard = find_or_create_dashboard(token, card_ids, collection)
    attach_cards(token, dashboard, card_ids)

    print("Done. Reopen the dashboard in Metabase to see the charts.")


if __name__ == "__main__":
    main()
