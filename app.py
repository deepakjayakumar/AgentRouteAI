import streamlit as st
import snowflake.connector
import math
import pandas as pd
import json
import base64
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Hide Streamlit header, footer, and the profile avatar
hide_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    /* This targets the user profile/identity icon in the bottom right */
    .st-emotion-cache-1wb593a {display: none !important;}
    .stAppDeployButton {display: none !important;}
    </style>
    """
st.markdown(hide_style, unsafe_allow_html=True)
load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "deepakjayakumar11@gmail.com")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def run_query(sql):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    return pd.DataFrame(data, columns=columns)


def run_execute(sql):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


st.set_page_config(page_title="Agentic AI for Delivery Execution", page_icon="\U0001f916", layout="wide")

st.markdown("""
<style>
    .main-header {
        background: white;
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        text-align: center;
        border: 2px solid #E61A27;
        box-shadow: 0 4px 12px rgba(230,26,39,0.1);
    }
    .main-header h1 { color: #E61A27; margin: 0.5rem 0 0 0; font-size: 2rem; }
    .main-header p { color: #666; margin: 0.3rem 0 0 0; font-size: 1rem; }
    .metric-card {
        background: white;
        border: 1px solid #E8E8E8;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .metric-card h3 { color: #E61A27; font-size: 2rem; margin: 0; }
    .metric-card p { color: #666; font-size: 0.85rem; margin: 0.3rem 0 0 0; }
    .store-card {
        background: white;
        border: 1px solid #E8E8E8;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.4rem 0;
    }
    .driver-card {
        background: #FFF5F5;
        border: 1px solid #FFD4D4;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.4rem 0;
    }
    .warehouse-badge {
        background: #E61A27;
        color: white;
        padding: 2px 8px;
        border-radius: 20px;
        font-size: 0.8rem;
        display: inline-block;
    }
    .chat-user {
        background: #F0F0F0;
        border-radius: 12px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
    }
    .chat-bot {
        background: #FFF5F5;
        border-left: 3px solid #E61A27;
        border-radius: 12px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
    }
    .save-success {
        background: #D4EDDA;
        border: 1px solid #C3E6CB;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        color: #155724;
    }
</style>
""", unsafe_allow_html=True)

logo_path = "agent_route_ai_logo.png"
if os.path.exists(logo_path):
    with open(logo_path, "rb") as f:
        logo_b64 = base64.b64encode(f.read()).decode("utf-8")
    st.markdown(f"""
    <div class="main-header">
        <img src="data:image/png;base64,{logo_b64}" alt="AgentRouteAI" style="height:80px; margin-bottom:0.3rem;" />
        <h1>Agentic AI for Delivery Execution</h1>
        <p>Intelligent delivery route optimization from warehouse to store</p>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="main-header">
        <h1>Agentic AI for Delivery Execution</h1>
        <p>Intelligent delivery route optimization from warehouse to store</p>
    </div>
    """, unsafe_allow_html=True)


def haversine(lat1, lon1, lat2, lon2):
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def road_dist(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2) * 1.3


def drive_hrs(dist, speed=55):
    return round(dist / speed, 2)


WEIGHT_PER_UNIT = 0.5
LOAD_TIME = 0.25
UNLOAD_TIME = 0.33


def extract_delivery_json(text):
    marker_start = "<!--DELIVERY_JSON:"
    marker_end = ":DELIVERY_JSON-->"
    idx_s = text.find(marker_start)
    idx_e = text.find(marker_end)
    if idx_s == -1 or idx_e == -1:
        return text, None
    json_str = text[idx_s + len(marker_start):idx_e]
    display_text = text[:idx_s].rstrip()
    try:
        records = json.loads(json_str)
        return display_text, records
    except Exception:
        return display_text, None


def generate_route_plan_local(instruction, orders_df, stores_df, drivers_df, wh_df):
    wh_map = {}
    for _, w in wh_df.iterrows():
        wh_map[w["WAREHOUSE_ID"]] = {"name": w["WAREHOUSE_NAME"], "city": w["CITY"], "lat": float(w["LATITUDE"]), "lon": float(w["LONGITUDE"])}

    def nearest_wh(lat, lon):
        best_id, best_d = None, float("inf")
        for wid, w in wh_map.items():
            d = road_dist(lat, lon, w["lat"], w["lon"])
            if d < best_d:
                best_id, best_d = wid, d
        return best_id

    store_map = {}
    for _, s in stores_df.iterrows():
        store_map[s["STORE_ID"]] = {"name": s["STORE_NAME"], "city": s["CITY"], "lat": float(s["LATITUDE"]), "lon": float(s["LONGITUDE"])}

    store_orders = {}
    for _, o in orders_df.iterrows():
        sid = o["STORE_ID"]
        if sid not in store_orders:
            store_orders[sid] = []
        store_orders[sid].append({"order_id": int(o["ORDER_ID"]), "product": o["PRODUCT_NAME"], "qty": int(o["QUANTITY"]), "date": str(o["ORDER_DATE"])})

    wh_stops = {}
    for sid, ords in store_orders.items():
        loc = store_map.get(sid)
        if not loc:
            continue
        assigned_wh = nearest_wh(loc["lat"], loc["lon"])
        if assigned_wh not in wh_stops:
            wh_stops[assigned_wh] = []
        total_qty = sum(o["qty"] for o in ords)
        wh_stops[assigned_wh].append({
            "store_id": sid, "store_name": loc["name"], "city": loc["city"],
            "lat": loc["lat"], "lon": loc["lon"], "orders": ords,
            "total_qty": total_qty, "weight_kg": round(total_qty * WEIGHT_PER_UNIT, 1)
        })

    wh_drivers = {}
    for _, d in drivers_df.iterrows():
        hw = d.get("HOME_WAREHOUSE_ID", "WH_PHX_01")
        if hw is None:
            hw = "WH_PHX_01"
        if hw not in wh_drivers:
            wh_drivers[hw] = []
        wh_drivers[hw].append({
            "id": int(d["DRIVER_ID"]), "name": d["DRIVER_NAME"],
            "capacity_kg": float(d["VEHICLE_CAPACITY_KG"]), "hours": float(d["HOURS_AVAILABLE"]),
            "route": [], "load_kg": 0.0, "time_used": 0.0, "distance": 0.0
        })

    all_unassigned = []
    delivery_records = []
    report = []
    report.append("# COCA-COLA SUPPLY CHAIN - MULTI-WAREHOUSE OPTIMIZED ROUTE PLAN")
    report.append("**Algorithm:** Nearest-Neighbor Heuristic with Multi-Warehouse Dispatch")
    report.append("**Config:** " + str(WEIGHT_PER_UNIT) + " kg/unit | 55 mph avg | 1.3x road factor | " + str(LOAD_TIME) + "h load | " + str(UNLOAD_TIME) + "h unload/stop")
    report.append("")

    total_orders_assigned = 0
    total_units_assigned = 0
    drivers_used = 0
    total_drivers = 0

    for wid in sorted(wh_map.keys()):
        w = wh_map[wid]
        stops = wh_stops.get(wid, [])
        drvrs = wh_drivers.get(wid, [])
        total_drivers += len(drvrs)

        report.append("---")
        report.append("# " + w["name"] + " (" + w["city"] + ")")
        report.append("**Warehouse ID:** " + wid + " | **Stores Served:** " + str(len(stops)) + " | **Drivers:** " + str(len(drvrs)))
        report.append("")

        if not stops:
            report.append("*No pending orders for this region.*")
            report.append("")
            continue
        if not drvrs:
            report.append("*No drivers assigned to this warehouse.*")
            for s in stops:
                all_unassigned.append(s)
            report.append("")
            continue

        stops_sorted = sorted(stops, key=lambda x: road_dist(w["lat"], w["lon"], x["lat"], x["lon"]))
        assigned_here = set()

        for driver in drvrs:
            cur_lat, cur_lon = w["lat"], w["lon"]
            remaining = driver["hours"] - LOAD_TIME
            stop_seq = 0

            while True:
                best, best_d = None, float("inf")
                for stop in stops_sorted:
                    if stop["store_id"] in assigned_here:
                        continue
                    if driver["load_kg"] + stop["weight_kg"] > driver["capacity_kg"]:
                        continue
                    d2 = road_dist(cur_lat, cur_lon, stop["lat"], stop["lon"])
                    t2 = drive_hrs(d2)
                    ret_d = road_dist(stop["lat"], stop["lon"], w["lat"], w["lon"])
                    ret_t = drive_hrs(ret_d)
                    if t2 + UNLOAD_TIME + ret_t > remaining:
                        continue
                    if d2 < best_d:
                        best, best_d = stop, d2

                if not best:
                    break

                d2 = road_dist(cur_lat, cur_lon, best["lat"], best["lon"])
                t2 = drive_hrs(d2)
                stop_seq += 1
                driver["route"].append({
                    "store_id": best["store_id"], "store_name": best["store_name"], "city": best["city"],
                    "orders": best["orders"], "total_qty": best["total_qty"], "weight_kg": best["weight_kg"],
                    "leg_mi": round(d2, 1), "leg_hrs": round(t2, 2),
                    "lat": best["lat"], "lon": best["lon"]
                })
                delivery_records.append({
                    "warehouse_id": wid,
                    "warehouse_name": w["name"],
                    "driver_id": driver["id"],
                    "driver_name": driver["name"],
                    "store_id": best["store_id"],
                    "store_name": best["store_name"],
                    "store_city": best["city"],
                    "order_ids": ", ".join(["#" + str(o["order_id"]) for o in best["orders"]]),
                    "total_quantity": best["total_qty"],
                    "weight_kg": best["weight_kg"],
                    "distance_mi": round(d2, 1),
                    "estimated_hours": round(t2, 2),
                    "stop_sequence": stop_seq
                })
                driver["load_kg"] += best["weight_kg"]
                driver["distance"] += d2
                driver["time_used"] += t2 + UNLOAD_TIME
                cur_lat, cur_lon = best["lat"], best["lon"]
                remaining -= (t2 + UNLOAD_TIME)
                assigned_here.add(best["store_id"])

            if driver["route"]:
                ret = road_dist(cur_lat, cur_lon, w["lat"], w["lon"])
                driver["distance"] = round(driver["distance"] + ret, 1)
                driver["time_used"] = round(driver["time_used"] + drive_hrs(ret) + LOAD_TIME, 2)
                drivers_used += 1

        for driver in drvrs:
            report.append("## " + driver["name"] + " (ID: " + str(driver["id"]) + ")")
            report.append("**Capacity:** " + str(driver["capacity_kg"]) + " kg | **Hours:** " + str(driver["hours"]) + "h")
            if not driver["route"]:
                report.append("*No deliveries assigned.*")
                report.append("")
                continue
            report.append("**Load:** " + str(round(driver["load_kg"], 1)) + " kg (" + str(round(driver["load_kg"]/driver["capacity_kg"]*100)) + "%) | **Distance:** " + str(driver["distance"]) + " mi | **Time:** " + str(driver["time_used"]) + "h")
            report.append("")
            report.append("| Stop | Store | City | Orders | Qty | Weight(kg) | Leg(mi) | Leg(hrs) |")
            report.append("|------|-------|------|--------|-----|-----------|---------|----------|")
            for i, stp in enumerate(driver["route"], 1):
                oids = ", ".join(["#" + str(o["order_id"]) for o in stp["orders"]])
                report.append("| " + str(i) + " | " + stp["store_name"] + " | " + stp["city"] + " | " + oids + " | " + str(stp["total_qty"]) + " | " + str(stp["weight_kg"]) + " | " + str(stp["leg_mi"]) + " | " + str(stp["leg_hrs"]) + " |")
                total_orders_assigned += len(stp["orders"])
                total_units_assigned += stp["total_qty"]
            last = driver["route"][-1]
            rb = round(road_dist(last["lat"], last["lon"], w["lat"], w["lon"]), 1)
            report.append("| RET | **WAREHOUSE** | " + w["city"] + " | - | - | - | " + str(rb) + " | " + str(drive_hrs(rb)) + " |")
            report.append("")

        for s in stops_sorted:
            if s["store_id"] not in assigned_here:
                all_unassigned.append(s)

    report.append("---")
    report.append("## Summary")
    report.append("| Metric | Value |")
    report.append("|--------|-------|")
    report.append("| Total Pending Orders | " + str(len(orders_df)) + " |")
    report.append("| Orders Assigned | " + str(total_orders_assigned) + " |")
    report.append("| Units Assigned | " + str(total_units_assigned) + " |")
    report.append("| Drivers Used | " + str(drivers_used) + "/" + str(total_drivers) + " |")
    fulfillment = round(total_orders_assigned / len(orders_df) * 100) if len(orders_df) > 0 else 0
    report.append("| Fulfillment Rate | " + str(fulfillment) + "% |")
    report.append("")

    if all_unassigned:
        report.append("## Unassigned Stops")
        for u in all_unassigned:
            oids = ", ".join(["#" + str(o["order_id"]) for o in u["orders"]])
            report.append("- **" + u["store_name"] + "** (" + u["city"] + "): " + str(u["total_qty"]) + " units / " + str(u["weight_kg"]) + " kg - Orders: " + oids)
        report.append("")

    plan_text = "\n".join(report)
    json_marker = "\n<!--DELIVERY_JSON:" + json.dumps(delivery_records) + ":DELIVERY_JSON-->"

    llm_summary = ""
    agent_log = []
    agent_log.append("## Agent Thinking Process\n")

    agent_log.append("**Analyzing pending orders...**")
    order_by_store = orders_df.groupby("STORE_ID").agg({"ORDER_ID": "count", "QUANTITY": "sum"}).reset_index()
    top_stores = order_by_store.sort_values("QUANTITY", ascending=False).head(3)
    store_insights = []
    for _, row in top_stores.iterrows():
        sname = store_map.get(row["STORE_ID"], {}).get("name", row["STORE_ID"])
        store_insights.append(f"{sname} ({int(row['QUANTITY'])} units across {int(row['ORDER_ID'])} orders)")
    agent_log.append(f"Found {len(orders_df)} pending orders totaling {int(orders_df['QUANTITY'].sum())} units across {len(order_by_store)} stores. "
                     f"Highest demand: {', '.join(store_insights)}.")

    agent_log.append("\n**Evaluating driver fleet...**")
    for _, d in drivers_df.iterrows():
        max_range_hrs = float(d["HOURS_AVAILABLE"]) - LOAD_TIME - UNLOAD_TIME
        max_range_mi = round(max_range_hrs * 55, 1)
        utilization_potential = round(float(d["VEHICLE_CAPACITY_KG"]) / (orders_df["QUANTITY"].sum() * WEIGHT_PER_UNIT) * 100, 1) if orders_df["QUANTITY"].sum() > 0 else 0
        agent_log.append(f"- **{d['DRIVER_NAME']}**: {d['VEHICLE_CAPACITY_KG']}kg capacity, {d['HOURS_AVAILABLE']}h available → effective range ~{max_range_mi} mi, "
                         f"can carry up to {utilization_potential}% of total demand.")

    agent_log.append("\n**Mapping stores to nearest warehouses...**")
    for wid in sorted(wh_map.keys()):
        w = wh_map[wid]
        stops = wh_stops.get(wid, [])
        if stops:
            distances = [round(road_dist(w["lat"], w["lon"], s["lat"], s["lon"]), 1) for s in stops]
            store_names = [s["store_name"] for s in stops]
            total_wt = sum(s["weight_kg"] for s in stops)
            agent_log.append(f"- **{w['name']}** ({w['city']}): assigned {len(stops)} stores "
                             f"[{', '.join(store_names)}], total load {total_wt}kg, "
                             f"distances range {min(distances)}–{max(distances)} mi.")

    agent_log.append("\n**Building routes using nearest-neighbor heuristic...**")
    for wid in sorted(wh_map.keys()):
        w = wh_map[wid]
        drvrs = wh_drivers.get(wid, [])
        for driver in drvrs:
            if driver["route"]:
                agent_log.append(f"- **{driver['name']}** starting from {w['name']}:")
                cumulative_load = 0.0
                cumulative_time = LOAD_TIME
                cumulative_dist = 0.0
                for stp in driver["route"]:
                    cumulative_load += stp["weight_kg"]
                    cumulative_dist += stp["leg_mi"]
                    cumulative_time += stp["leg_hrs"] + UNLOAD_TIME
                    remaining_capacity = round(driver["capacity_kg"] - cumulative_load, 1)
                    remaining_time = round(driver["hours"] - cumulative_time, 2)
                    agent_log.append(f"  → Nearest feasible stop: **{stp['store_name']}** ({stp['city']}) — "
                                     f"{stp['leg_mi']} mi from previous point, {stp['total_qty']} units/{stp['weight_kg']}kg. "
                                     f"Remaining capacity: {remaining_capacity}kg, "
                                     f"time budget after this: {remaining_time}h.")
                ret_dist = round(road_dist(driver["route"][-1]["lat"], driver["route"][-1]["lon"], w["lat"], w["lon"]), 1)
                ret_time = drive_hrs(ret_dist)
                cumulative_dist += ret_dist
                cumulative_time += ret_time
                agent_log.append(f"  ← Return to warehouse ({ret_dist} mi, {ret_time}h). "
                                 f"Trip total: {round(cumulative_dist, 1)} mi, {round(cumulative_time, 2)}h, "
                                 f"{round(cumulative_load, 1)}kg ({round(cumulative_load/driver['capacity_kg']*100)}% loaded).")
            else:
                agent_log.append(f"- **{driver['name']}**: No feasible stops could be assigned (capacity/time constraints).")

    if all_unassigned:
        agent_log.append("\n**Identifying unassigned stops...**")
        for u in all_unassigned:
            nearest_wid = nearest_wh(u["lat"], u["lon"])
            wh_name = wh_map[nearest_wid]["name"]
            dist_to_wh = round(road_dist(wh_map[nearest_wid]["lat"], wh_map[nearest_wid]["lon"], u["lat"], u["lon"]), 1)
            agent_log.append(f"- **{u['store_name']}** ({u['city']}): {u['weight_kg']}kg, {dist_to_wh} mi from {wh_name} — "
                             f"skipped because all drivers at {wh_name} ran out of capacity or time.")

    agent_log.append(f"\n**Final assessment:** Assigned {total_orders_assigned}/{len(orders_df)} orders ({fulfillment}% fulfillment) "
                     f"using {drivers_used}/{total_drivers} drivers. "
                     f"Total units: {total_units_assigned}, unassigned stops: {len(all_unassigned)}.")

    agent_log_text = "\n".join(agent_log)
    return agent_log_text + "\n\n" + plan_text + json_marker
    # if GOOGLE_API_KEY:
    #     try:
    #         import google.generativeai as genai
    #         genai.configure(api_key=GOOGLE_API_KEY)
    #         model = genai.GenerativeModel("gemini-3-flash-preview")
    #         summary_prompt = (
    #             "You are a senior logistics analyst for Coca-Cola. Based on this route plan summary, write: "
    #             "1) A 3-sentence executive summary assessing overall efficiency and fulfillment. "
    #             "2) A section titled '## Actionable Recommendations' with 4-5 specific bullet points. "
    #             "Return ONLY the summary and recommendations, nothing else. "
    #             f"Plan summary: {total_orders_assigned} of {len(orders_df)} orders assigned, "
    #             f"{total_units_assigned} units, {drivers_used}/{total_drivers} drivers used, "
    #             f"{fulfillment}% fulfillment, {len(all_unassigned)} unassigned stops. "
    #             f"User request: {instruction}"
    #         )
    #         response = model.generate_content(summary_prompt)
    #         llm_summary = response.text
    #     except Exception:
    #         pass

    # if llm_summary:
    #     return llm_summary + "\n\n" + plan_text + json_marker
    # return plan_text + json_marker


def save_delivery_plan(records):
    if not records:
        return 0
    values_list = []
    for r in records:
        wh_id = str(r.get("warehouse_id", "")).replace("'", "''")
        wh_name = str(r.get("warehouse_name", "")).replace("'", "''")
        d_id = r.get("driver_id", 0)
        d_name = str(r.get("driver_name", "")).replace("'", "''")
        s_id = str(r.get("store_id", "")).replace("'", "''")
        s_name = str(r.get("store_name", "")).replace("'", "''")
        s_city = str(r.get("store_city", "")).replace("'", "''")
        o_ids = str(r.get("order_ids", "")).replace("'", "''")
        qty = r.get("total_quantity", 0)
        wt = r.get("weight_kg", 0)
        dist = r.get("distance_mi", 0)
        hrs = r.get("estimated_hours", 0)
        seq = r.get("stop_sequence", 0)
        values_list.append(
            f"(CURRENT_TIMESTAMP(), '{wh_id}', '{wh_name}', {d_id}, '{d_name}', "
            f"'{s_id}', '{s_name}', '{s_city}', '{o_ids}', {qty}, {wt}, {dist}, {hrs}, {seq}, 'Planned')"
        )
    values_sql = ", ".join(values_list)
    insert_sql = (
        f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.DELIVERY_DETAILS "
        "(PLAN_TIMESTAMP, WAREHOUSE_ID, WAREHOUSE_NAME, DRIVER_ID, DRIVER_NAME, "
        "STORE_ID, STORE_NAME, STORE_CITY, ORDER_IDS, TOTAL_QUANTITY, WEIGHT_KG, "
        "DISTANCE_MI, ESTIMATED_HOURS, STOP_SEQUENCE, STATUS) VALUES " + values_sql
    )
    run_execute(insert_sql)
    return len(records)


def send_plan_email(records):
    if not records or not EMAIL_SENDER or not EMAIL_PASSWORD:
        return
    rows_html = ""
    total_qty = 0
    total_weight = 0
    for r in records:
        rows_html += (
            "<tr>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('driver_name','')}</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('store_name','')}</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('store_city','')}</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('order_ids','')}</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('total_quantity',0)}</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('weight_kg',0)} kg</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('distance_mi',0)} mi</td>"
            f"<td style='padding:8px;border:1px solid #ddd'>{r.get('estimated_hours',0)} hrs</td>"
            "</tr>"
        )
        total_qty += r.get('total_quantity', 0)
        total_weight += r.get('weight_kg', 0)
    num_stops = len(records)
    drivers_used = len(set(r.get('driver_name', '') for r in records))
    html_body = (
        "<html><body style='font-family:Arial,sans-serif'>"
        "<div style='background:#E61A27;padding:20px;text-align:center'>"
        "<h1 style='color:white;margin:0'>Delivery Plan Approved</h1>"
        "<p style='color:#FFD4D4;margin:5px 0 0 0'>Agentic AI for Delivery Execution</p>"
        "</div>"
        "<div style='padding:20px'>"
        "<h2 style='color:#333'>Plan Summary</h2>"
        "<table style='border-collapse:collapse;margin:10px 0'>"
        f"<tr><td style='padding:5px 15px;font-weight:bold'>Total Stops:</td><td>{num_stops}</td></tr>"
        f"<tr><td style='padding:5px 15px;font-weight:bold'>Drivers Assigned:</td><td>{drivers_used}</td></tr>"
        f"<tr><td style='padding:5px 15px;font-weight:bold'>Total Quantity:</td><td>{total_qty} units</td></tr>"
        f"<tr><td style='padding:5px 15px;font-weight:bold'>Total Weight:</td><td>{total_weight} kg</td></tr>"
        "</table>"
        "<h2 style='color:#333'>Delivery Details</h2>"
        "<table style='border-collapse:collapse;width:100%'>"
        "<tr style='background:#E61A27;color:white'>"
        "<th style='padding:10px;border:1px solid #ddd'>Driver</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Store</th>"
        "<th style='padding:10px;border:1px solid #ddd'>City</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Order IDs</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Qty</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Weight</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Distance</th>"
        "<th style='padding:10px;border:1px solid #ddd'>Est. Time</th>"
        "</tr>"
        f"{rows_html}"
        "</table>"
        "<p style='color:#888;margin-top:20px;font-size:12px'>This is an automated email from Agentic AI for Delivery Execution.</p>"
        "</div></body></html>"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Delivery Plan Approved - Coca-Cola Route Planner"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())


def find_excluded_drivers(user_msg, drivers_df):
    msg_lower = user_msg.lower()
    excluded = []
    for _, d in drivers_df.iterrows():
        full_name = str(d["DRIVER_NAME"]).lower()
        first_name = full_name.split()[0] if full_name.split() else ""
        last_name = full_name.split()[-1] if len(full_name.split()) > 1 else ""
        if full_name in msg_lower or (first_name and first_name in msg_lower and len(first_name) > 2) or (last_name and last_name in msg_lower and len(last_name) > 2):
            excluded.append(int(d["DRIVER_ID"]))
    return excluded


def call_route_plan(user_input, excluded_driver_ids, orders_df, stores_df, drivers_df, warehouses_df):
    filtered_drivers = drivers_df.copy()
    if excluded_driver_ids:
        filtered_drivers = filtered_drivers[~filtered_drivers["DRIVER_ID"].isin(excluded_driver_ids)]
    pending = orders_df[orders_df["ORDER_STATUS"] == "Pending"].sort_values(["ORDER_DATE", "ORDER_ID"])
    return generate_route_plan_local(user_input, pending, stores_df, filtered_drivers, warehouses_df)


def is_followup(msg):
    lower = msg.lower()
    keywords = [
        "not available", "unavailable", "remove", "exclude", "without",
        "skip", "can't", "cannot", "isn't", "don't use", "don't include",
        "is sick", "is off", "is absent", "is busy", "take out", "drop",
        "recreate", "redo", "regenerate", "re-create", "re-do", "re-generate"
    ]
    return any(k in lower for k in keywords)


def load_data():
    o = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ORDER_DETAILS_NEW")
    s = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STORE_DETAILS")
    d = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.DRIVER_DETAILS")
    w = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.WAREHOUSE_DETAILS")
    return o, s, d, w


orders, stores, drivers, warehouses = load_data()
pending = orders[orders["ORDER_STATUS"] == "Pending"]

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(
        f'<div class="metric-card"><h3>{len(pending)}</h3><p>Pending Orders</p></div>',
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f'<div class="metric-card"><h3>{int(pending["QUANTITY"].sum()):,}</h3><p>Total Units</p></div>',
        unsafe_allow_html=True,
    )
with c3:
    st.markdown(
        f'<div class="metric-card"><h3>{len(drivers)}</h3><p>Available Drivers</p></div>',
        unsafe_allow_html=True,
    )
with c4:
    st.markdown(
        f'<div class="metric-card"><h3>{int(drivers["VEHICLE_CAPACITY_KG"].sum())} kg</h3><p>Fleet Capacity</p></div>',
        unsafe_allow_html=True,
    )

st.markdown("")
tab1, tab2, tab3, tab4 = st.tabs(
    ["\U0001f4ac Route Planner Chat", "\U0001f4e6 Orders Dashboard", "\U0001f69a Fleet Overview", "\U0001f3ea Store Network"]
)

with tab1:
    left, right = st.columns([2, 1])
    with right:
        st.markdown("##### Quick Actions")
        prompts = [
            "Generate a full route plan for all pending orders",
            "Plan routes only for the earliest pending orders",
            "Optimize routes prioritizing Phoenix and Tucson stores",
            "Create a delivery plan for the driver with most capacity",
        ]
        picked = None
        for p in prompts:
            if st.button(p, use_container_width=True, key=f"q_{p[:15]}"):
                picked = p
        st.markdown("---")
        st.markdown("##### Warehouse Hub")
        for _, wh in warehouses.iterrows():
            st.markdown(
                f'<div class="store-card"><span class="warehouse-badge">HUB</span> <strong>{wh["WAREHOUSE_NAME"]}</strong><br>\U0001f4cd {wh["CITY"]}</div>',
                unsafe_allow_html=True,
            )
        st.markdown("##### Driver Fleet")
        for _, d in drivers.iterrows():
            st.markdown(
                f'<div class="driver-card"><strong>{d["DRIVER_NAME"]}</strong><br>\U0001f69b {d["VEHICLE_CAPACITY_KG"]}kg | \u23f1 {d["HOURS_AVAILABLE"]}h</div>',
                unsafe_allow_html=True,
            )
    with left:
        st.markdown("##### Ask the Route Planner")
        if "msgs" not in st.session_state:
            st.session_state.msgs = []
        if "delivery_data" not in st.session_state:
            st.session_state.delivery_data = {}
        if "saved_plans" not in st.session_state:
            st.session_state.saved_plans = set()
        if "excluded_drivers" not in st.session_state:
            st.session_state.excluded_drivers = []

        for idx, m in enumerate(st.session_state.msgs):
            if m["role"] == "user":
                st.markdown(
                    f'<div class="chat-user">\U0001f9d1 <strong>You:</strong> {m["content"]}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="chat-bot">\U0001f916 <strong>Route Planner:</strong></div>',
                    unsafe_allow_html=True,
                )
                st.markdown(m["content"])
                msg_key = f"save_{idx}"
                if msg_key in st.session_state.delivery_data and msg_key not in st.session_state.saved_plans:
                    if st.button("\U0001f4be Approve the Delivery Plan ", key=msg_key, use_container_width=True, type="primary"):
                        records = st.session_state.delivery_data[msg_key]
                        count = save_delivery_plan(records)
                        try:
                            send_plan_email(records)
                            st.toast(f"Email sent to {EMAIL_RECIPIENT}")
                        except Exception as e:
                            st.warning(f"Plan saved but email failed: {e}")
                        st.session_state.saved_plans.add(msg_key)
                        st.rerun()
                elif msg_key in st.session_state.saved_plans:
                    st.markdown(
                        '<div class="save-success">\u2705 Delivery plan saved to DELIVERY_DETAILS table & email notification sent!</div>',
                        unsafe_allow_html=True,
                    )

        with st.form("chat_form", clear_on_submit=True):
            user_input = st.text_input(
                "Type your request and press Enter:",
                key="chat_input",
                placeholder="e.g. Generate route plan for all pending orders",
            )
            submitted = st.form_submit_button("\U0001f680 Generate Plan", type="primary", use_container_width=True)
        if picked:
            user_input = picked
            submitted = True
        if submitted and user_input:
            st.session_state.msgs.append({"role": "user", "content": user_input})
            with st.spinner("\U0001f504 Generating optimized route plan..."):
                excluded = list(st.session_state.excluded_drivers)
                if is_followup(user_input):
                    new_exclusions = find_excluded_drivers(user_input, drivers)
                    for eid in new_exclusions:
                        if eid not in excluded:
                            excluded.append(eid)
                    st.session_state.excluded_drivers = excluded
                raw_reply = call_route_plan(user_input, excluded if excluded else None, orders, stores, drivers, warehouses)
            display_text, delivery_records = extract_delivery_json(raw_reply)
            if excluded:
                excluded_names = drivers[drivers["DRIVER_ID"].isin(excluded)]["DRIVER_NAME"].tolist()
                if excluded_names:
                    display_text = "**Excluded drivers:** " + ", ".join(excluded_names) + "\n\n" + display_text
            st.session_state.msgs.append({"role": "assistant", "content": display_text})
            if delivery_records:
                msg_idx = len(st.session_state.msgs) - 1
                st.session_state.delivery_data[f"save_{msg_idx}"] = delivery_records
            st.rerun()
        if st.button("\U0001f5d1 Clear Chat", use_container_width=True):
            st.session_state.msgs = []
            st.session_state.delivery_data = {}
            st.session_state.saved_plans = set()
            st.session_state.excluded_drivers = []
            st.rerun()

with tab2:
    ca, cb = st.columns([1, 2])
    with ca:
        st.markdown("##### Filter Orders")
        all_dates = sorted(pending["ORDER_DATE"].astype(str).unique())
        sel_dates = st.multiselect("Order Date", all_dates, default=all_dates)
        all_prods = sorted(pending["PRODUCT_NAME"].unique())
        sel_prods = st.multiselect("Product", all_prods, default=all_prods)
    filtered = pending[
        (pending["ORDER_DATE"].astype(str).isin(sel_dates))
        & (pending["PRODUCT_NAME"].isin(sel_prods))
    ]
    with cb:
        st.markdown(f"##### Pending Orders ({len(filtered)})")
        st.dataframe(filtered, use_container_width=True)
    st.markdown("---")
    st.markdown("##### Orders by Product")
    ps = (
        filtered.groupby("PRODUCT_NAME")["QUANTITY"]
        .sum()
        .reset_index()
        .sort_values("QUANTITY", ascending=False)
    )
    st.bar_chart(ps.set_index("PRODUCT_NAME"))

with tab3:
    st.markdown("##### Driver Fleet Details")
    st.dataframe(drivers, use_container_width=True)
    st.markdown("---")
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("##### Vehicle Capacity (kg)")
        st.bar_chart(drivers[["DRIVER_NAME", "VEHICLE_CAPACITY_KG"]].set_index("DRIVER_NAME"))
    with d2:
        st.markdown("##### Hours Available")
        st.bar_chart(drivers[["DRIVER_NAME", "HOURS_AVAILABLE"]].set_index("DRIVER_NAME"))

with tab4:
    st.markdown("##### Store Locations")
    st.dataframe(stores, use_container_width=True)
    if not stores.empty and not warehouses.empty:
        mp = stores.rename(columns={"LATITUDE": "latitude", "LONGITUDE": "longitude"})
        wm = warehouses.rename(columns={"LATITUDE": "latitude", "LONGITUDE": "longitude"})
        all_pts = pd.concat(
            [mp[["latitude", "longitude"]], wm[["latitude", "longitude"]]], ignore_index=True
        )
        st.markdown("##### Store & Warehouse Map")
        st.map(all_pts)
        st.markdown("---")
        wh = warehouses.iloc[0]
        st.markdown("##### Distance from Warehouse")
        dists = []
        for _, s in stores.iterrows():
            dm = haversine(
                float(wh["LATITUDE"]),
                float(wh["LONGITUDE"]),
                float(s["LATITUDE"]),
                float(s["LONGITUDE"]),
            )
            dists.append(
                {"Store": s["STORE_NAME"], "City": s["CITY"], "Distance (mi)": round(dm, 1)}
            )
        st.dataframe(
            pd.DataFrame(dists).sort_values("Distance (mi)"),
            use_container_width=True,
        )
