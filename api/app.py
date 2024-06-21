import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection import get_db_connection

from flask import Flask, jsonify, request, render_template, redirect, url_for, session, flash
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from collections import OrderedDict
import ast
import json
from prettytable import PrettyTable
from datetime import datetime, timedelta, timezone
import pytz
import math
import random
import logging
import click

import mysql.connector
from mysql.connector import Error

import re
from beem import Steem
from steemengine.market import Wallet

import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 1
app.config.from_object(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

CORS(app)

upgrade_keys = ["shipyard", "oredepot", "copperdepot", "coaldepot", "uraniumdepot", "explorership",
                "transportship", "scout","patrol","cutter","corvette", "frigate", "destroyer", "cruiser", "battlecruiser",
                "carrier", "dreadnought","yamato", "yamato1", "yamato2", "yamato3","yamato4","yamato5","yamato6","yamato7","yamato8","yamato9","yamato10","yamato11","yamato12",
                      "yamato13","yamato14","yamato15","yamato16","yamato17","yamato18","yamato19","yamato20", "oremine", "coppermine", "coalmine", "uraniummine", "base",
                "researchcenter", "bunker", "shieldgenerator", "transportship1", "transportship2"]


skill_keys = ["shipyard", "oredepot", "copperdepot", "coaldepot", "uraniumdepot", "Explorer",
                "Transporter", "Scout", "Patrol", "Cutter", "Corvette", "Frigate", "Destroyer", "Cruiser", "Battlecruiser",
                "Carrier", "Dreadnought", "Yamato", "oremine", "coppermine", "coalmine", "uraniummine", "base", "researchcenter",
                "orebooster", "coalbooster", "copperbooster", "uraniumbooster", "missioncontrol", "bunker", "enlargebunker",
                "structureimprove", "armorimprove", "shieldimprove",
                "rocketimprove", "bulletimprove", "laserimprove", "regenerationbonus", "repairbonus", "shieldgenerator",
                "siegeprolongation", "depotincrease"]

@app.route('/sendCommand', methods=['POST'])
def SendCommand():
    connection = get_db_connection()

    try:
        data = request.get_json()
        
        if connection is None:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = connection.cursor()

        insert_query = """
            INSERT INTO transactions (
                trx, user, tr_type, tr_var1, tr_var2, tr_var3, tr_var4, tr_var5, tr_var6, tr_var7, tr_var8, tr_status, createdAt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        """

        values = (
            data.get("trxId", ""),
            data.get("username", ""),
            data.get("tr_type", ""),
            data.get("tr_var1", ""),
            data.get("tr_var2", ""),
            data.get("tr_var3", ""),
            data.get("tr_var4", ""),
            data.get("tr_var5", ""),
            data.get("tr_var6", ""),
            data.get("tr_var7", ""),
            data.get("tr_var8", ""),
            0
        )

        cursor.execute(insert_query, values)

        return jsonify({"message": "Transaction inserted successfully."}), 200

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return jsonify({"error": "Failed to insert transaction"}), 500
    
    except Exception as err:
        print(f"Error: {err}")
        return jsonify({"error": "Exception"}), 500

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close() 

def GetPlanetImg(rarity, type, id):
    img = ""
    #Rarity
    if rarity == "undefined" or rarity == "common":
        img = img + "co"
    
    if(rarity == "uncommon"):
        img = img + "un"
    
    if(rarity == "rare"):
        img = img + "rar"
    
    if(rarity == "legendary"):
        img = img + "leg"
    
    img = img + "_"
    if(type == "earth"):
        img = img + "atm"
    else:
        img = img + str(type)
    
    img = img + "_"
    if(id == 0):
        img = img + "1"
    else:
        img = img + str(id)
    
    img = img + ".png"
    return img
    

@app.route('/')
def main():
    return ""

# {"coal":71.3231,"ore":23.2371,"uranium":0.832567,"copper":2.7048,"coalrate":320,"orerate":120,"copperrate":40,"uraniumrate":20,"coaldepot":480,"oredepot":240,"copperdepot":60,"uraniumdepot":30,"lastUpdate":1555960626}
@app.route('/loadqyt', methods=['GET'])
def loadqyt():
    planetid = request.args.get('id', None)
    if not planetid :
        return jsonify({
            "coal": 0,
            "ore": 0,
            "copper": 0,
            "uranium": 0,
            "lastUpdate": 99999999999999
        })

    try:
        # Establish database connection
        connection = get_db_connection()
        if not connection:
            return jsonify({"error": "Failed to connect to database"}), 500

        cursor = connection.cursor(dictionary=True)

        # Query the planets table
        query = f"SELECT * FROM planets WHERE id = {planetid}"
        cursor.execute(query)
        p = cursor.fetchone()

        if p is None:
            return jsonify({
                "coal": 0,
                "ore": 0,
                "copper": 0,
                "uranium": 0,
                "lastUpdate": 99999999999999
            })

        # Return JSON response with queried data
        return jsonify({
            "coal": float(p["qyt_coal"]),
            "ore": float(p["qyt_ore"]),
            "copper": float(p["qyt_copper"]),
            "uranium": float(p["qyt_uranium"]),
            "coalrate": float(p["rate_coal"]),
            "orerate": float(p["rate_ore"]),
            "copperrate": float(p["rate_copper"]),
            "uraniumrate": float(p["rate_uranium"]),
            "coaldepot": float(p["depot_coal"]),
            "oredepot": float(p["depot_ore"]),
            "copperdepot": float(p["depot_copper"]),
            "uraniumdepot": float(p["depot_uranium"]),
            "lastUpdate": int(p["last_update"].timestamp())
        })

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadbuildings', methods=['GET'])
def loadbuildings():
    planetid = request.args.get('id', None)
    if not planetid:
        return jsonify([])

    try:
        # Establish database connection
        connection = get_db_connection()
        if not connection:
            return jsonify({"error": "Failed to connect to database"}), 500

        cursor = connection.cursor(dictionary=True)

        # Query the planets table
        query = f"SELECT * FROM planets WHERE id = {planetid}"
        cursor.execute(query)
        p = cursor.fetchone()

        if not p:
            return jsonify([])

        baseLevel = p["level_base"]
        user = p["user"]
        buildings = []
        types = []

        # Query the users table
        query = f"SELECT * FROM users WHERE username = '{user}'"
        cursor.execute(query)
        u = cursor.fetchone()

        shieldprotection_busy = p.get("shieldprotection_busy", 0)
        shieldcharge_busy = p.get("shieldcharge_busy", 0)
        shieldcharged = p.get("shieldcharged", 0)

        for orig_name in upgrade_keys:
            name = orig_name

            ressource = None
            if name.find("mine") > -1:
                name = name.replace("mine", "")
                ressource = name
            elif name.find("depot") > -1:
                ressource = name.replace("depot", "")
            elif name.find("center") > -1:
                name = name.replace("center", "")

            if "level_" + name not in p:
                current = 0
                time_busy = False
                cur_skill = 0
                continue
            else:
                current = p["level_" + name]
                if current is None:
                    current = 0
                    time_busy = False
                    cur_skill = 0
                if name + "_busy" in p and p[name + "_busy"] is not None:
                    time_busy = int(p[name + "_busy"].timestamp())
                if "r_" + orig_name in u:
                    cur_skill = u["r_" + orig_name]

            # Query the productivity table
            query = f"SELECT * FROM productivity WHERE name = '{orig_name}' AND level = {current}"
            cursor.execute(query)
            rr = cursor.fetchone()

            query = f"SELECT * FROM productivity WHERE name = '{orig_name}' AND level = {current + 1}"
            cursor.execute(query)
            rrd = cursor.fetchone()

            if ressource is not None:
                cur_rate = rr[ressource] if rr else None
                next_rate = rrd[ressource] if rrd else None
            else:
                cur_rate = None
                next_rate = None

            misc = None
            if orig_name == "shieldgenerator":
                misc = {"shieldprotection_busy": shieldprotection_busy, "shieldcharge_busy": shieldcharge_busy, "shieldcharged": shieldcharged}

            # Query the upgradecosts table
            query = f"SELECT * FROM upgradecosts WHERE name = '{orig_name}' AND level = {current + 1}"
            cursor.execute(query)
            r = cursor.fetchone()

            if r:
                realTime = (100 - baseLevel) / 100 * r["upgrade_time"]

                buildings.append({
                    "name": orig_name,
                    "current": current,
                    "coal": float(r["coal"]),
                    "ore": float(r["ore"]),
                    "copper": float(r["copper"]),
                    "uranium": float(r["uranium"]),
                    "research": 0,
                    "skill": cur_skill,
                    "cur_rate": cur_rate,
                    "next_rate": next_rate,
                    "base": 0,
                    "time": int(realTime),
                    "busy": time_busy,
                    "misc": misc
                })
            else:
                buildings.append({
                    "name": orig_name,
                    "current": current,
                    "coal": 0,
                    "ore": 0,
                    "copper": 0,
                    "uranium": 0,
                    "research": 0,
                    "skill": cur_skill,
                    "cur_rate": cur_rate,
                    "next_rate": next_rate,
                    "base": 0,
                    "time": int(0),
                    "busy": time_busy,
                    "misc": misc
                })

        # Return JSON response with buildings data
        return jsonify(buildings)

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loaduser', methods=['GET'])
def loaduser():
    user = request.args.get('user', None)
    if not user:
        return jsonify([])
    
    connection = get_db_connection()

    if connection is None:
        return jsonify({"error": "Failed to connect to database"}), 500
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (user,))
        user_data = cursor.fetchone()

        if user_data is None:
            return jsonify([])

        cursor.execute("SELECT SUM(stardust) AS totalStardust FROM users WHERE username != 'null'")
        stardust_supply = cursor.fetchone()["totalStardust"]

        return jsonify({
            "username": user_data["username"],
            "date": int(user_data["date"].timestamp()),
            "stardust": user_data["stardust"],
            "supply": stardust_supply
        })
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Failed to execute query"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/sd_balance', methods=['GET'])
def sd_balance():
    user = request.args.get('user', None)
    if not user:
        return jsonify([])

    try:
        wallet = Wallet(user)
        se_balance = float(wallet.get_token(symbol="STARDUST")['balance']) * 100000000
    except Exception as e:
        print(f"Error fetching STARDUST balance: {e}")
        se_balance = 0

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        query_user = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query_user, (user,))
        u = cursor.fetchone()

        query_supply = "SELECT SUM(stardust) AS total_stardust FROM users WHERE username != 'null'"
        cursor.execute(query_supply)
        row = cursor.fetchone()
        stardust_supply = int(row['total_stardust']) if row and row['total_stardust'] else 0

        if u is None:
            return jsonify([])

        return jsonify({
            "username": u["username"],
            "date": int(u["date"].timestamp()),
            "stardust": u["stardust"],
            "se_stardust": int(se_balance),
            "supply": stardust_supply
        })

    except mysql.connector.Error as e:
        print(f"Error fetching user details: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


@app.route('/currentseason', methods=['GET'])
def currentseason():
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        query_last_season = "SELECT * FROM season ORDER BY end_date DESC LIMIT 1"
        cursor.execute(query_last_season)
        last_season = cursor.fetchone()
        
        if last_season is None or last_season["end_date"] < datetime.utcnow():
            return jsonify({})

        return jsonify(last_season)

    except mysql.connector.Error as e:
        print(f"Error fetching current season: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/wallet', methods=['GET'])
def wallet():
    user = request.args.get('user', None)
    limit = request.args.get('limit', None)
    page = request.args.get('page', None)    
    
    if not user :
        return jsonify([])

    try:
        limit = int(limit) if limit else None
        page = int(page) if page else 0
    except ValueError:
        limit = None
        page = 0
    
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch user details
        query_user = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query_user, (user,))
        u = cursor.fetchone()

        if u is None:
            return jsonify([])

        # Fetch stardust supply
        query_supply = "SELECT SUM(stardust) AS total_stardust FROM users WHERE username != 'null'"
        cursor.execute(query_supply)
        stardust_supply = cursor.fetchone()['total_stardust'] or 0

        # Fetch transactions
        transactions = []
        query_transactions = "SELECT * FROM stardust WHERE from_user = %s OR to_user = %s"
        cursor.execute(query_transactions, (user, user))
        transactions = cursor.fetchall()

        # Convert timestamps to integers
        for transaction in transactions:
            transaction["date"] = int(transaction["date"].timestamp())

        # Sort transactions by date in descending order
        transactions = sorted(transactions, key=lambda m: m["date"], reverse=True)

        # Paginate transactions if limit and page are specified
        if limit is not None:
            start_idx = limit * page
            end_idx = limit * (page + 1)
            transactions = transactions[start_idx:end_idx]

        return jsonify({
            "username": u["username"],
            "date": int(u["date"].timestamp()),
            "stardust": u["stardust"],
            "supply": stardust_supply,
            "transactions": transactions
        })

    except mysql.connector.Error as e:
        print(f"Error fetching wallet data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/wallet_ranking', methods=['GET'])

def wallet_ranking():
    limit = request.args.get('limit', 150)
    page = request.args.get('page', None)
    
    try:
        limit = int(limit)
    except ValueError:
        limit = 150
    
    if page is not None:
        try:
            page = int(page)
        except ValueError:
            page = None
    
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch total stardust supply
        query_supply = "SELECT SUM(stardust) AS total_stardust FROM users WHERE username != 'null'"
        cursor.execute(query_supply)
        stardust_supply = cursor.fetchone()['total_stardust'] or 1  # Ensure stardust_supply is not zero

        # Fetch user rankings
        query_rankings = "SELECT username, stardust FROM users WHERE username != 'null' ORDER BY stardust DESC LIMIT %s"
        cursor.execute(query_rankings, (limit,))
        rows = cursor.fetchall()

        # Format rows into JSON response
        stack = []
        for row in rows:
            stack.append({
                "user": row["username"],
                "stardust": int(row["stardust"]),
                "percentage": row["stardust"] / stardust_supply
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching wallet rankings: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadtransaction', methods=['GET'])

def loadtransaction():
    trx_id = request.args.get('trx_id', None)
    if not trx_id :
        return jsonify([])

    limit = 10
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM transactions WHERE trx = %s LIMIT %s"
        cursor.execute(query, (trx_id, limit))
        trx_list = cursor.fetchall()

        for trx in trx_list:
            trx["date"] = int(trx["date"].timestamp())

        if len(trx_list) == 0:
            return jsonify([])

        return jsonify(trx_list)

    except mysql.connector.Error as e:
        print(f"Error fetching transactions: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadskills', methods=['GET'])
def loadskills():
    user = request.args.get('user', None)
    if not user:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query, (user,))
        u = cursor.fetchone()

        if u is None:
            return jsonify([])

        skills = []
        for name in skill_keys:
            current = u["r_" + name]
            if u["r_" + name + "_busy"] is None:
                time_busy = 0
            else:
                time_busy = int(u["r_" + name + "_busy"].timestamp())

            query_skill = "SELECT * FROM skillcosts WHERE name = %s AND level = %s"
            cursor.execute(query_skill, (name, current + 1))
            r = cursor.fetchone()

            if r is not None:
                skills.append({
                    "name": name,
                    "current": current,
                    "coal": float(r["coal"]),
                    "ore": float(r["ore"]),
                    "copper": float(r["copper"]),
                    "uranium": float(r["uranium"]),
                    "time": r["research_time"],
                    "busy": time_busy
                })
            else:
                skills.append({
                    "name": name,
                    "current": current,
                    "coal": 0,
                    "ore": 0,
                    "copper": 0,
                    "uranium": 0,
                    "time": 0,
                    "busy": time_busy
                })

        return jsonify(skills)

    except mysql.connector.Error as e:
        print(f"Error fetching skills: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadplanets', methods=['GET'])
def loadplanets():
    user = request.args.get('user', None)
    fromm = int(request.args.get('from', 0))
    to = int(request.args.get('to', 100))
    sort = request.args.get('sort', "id")
    if to == 0:
        to = 100

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)
        table_name = "planets"
        query_params = {
            "user": user,
            "fromm": fromm,
            "to": to,
            "sort": sort
        }

        if not user or user == "":
            query = f"SELECT * FROM {table_name} ORDER BY {sort} LIMIT %s"
            cursor.execute(query, (to,))
        else:
            query = f"SELECT * FROM {table_name} WHERE user = %(user)s ORDER BY {sort} LIMIT %(to)s"
            cursor.execute(query, query_params)

        planets = []
        for row in cursor.fetchall():
            planets.append({
                "name": row["name"],
                "username": row["user"],
                "id": row["id"],
                "posx": row["cords_hor"],
                "for_sale": row["for_sale"],
                "posy": row["cords_ver"],
                "starter": row["startplanet"],
                "bonus": row["bonus"],
                "planet_type": row["planet_type"],
                "date": int(row["date_disc"].timestamp())
            })

        return jsonify({"planets": planets, "misc": {"total": len(planets)}})

    except mysql.connector.Error as e:
        print(f"Error fetching planets: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadproduction', methods=['GET'])
def loadproduction():
    planetid = request.args.get('id', None)
    user = request.args.get('user', None)
    if not planetid or not user:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch planet details
        query_planet = "SELECT * FROM planets WHERE id = %s"
        cursor.execute(query_planet, (planetid,))
        row_planet = cursor.fetchone()
        if row_planet is None:
            return jsonify([])

        # Fetch user details
        query_user = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query_user, (user,))
        row_user = cursor.fetchone()
        if row_user is None:
            return jsonify([])

        # Extract necessary values from planet row
        bunker_level = row_planet['level_bunker']
        coalmine_level = row_planet['level_coal']
        oremine_level = row_planet['level_ore']
        coppermine_level = row_planet['level_copper']
        uraniummine_level = row_planet['level_uranium']
        coaldepot_size = float(row_planet['depot_coal'])
        oredepot_size = float(row_planet['depot_ore'])
        copperdepot_size = float(row_planet['depot_copper'])
        uraniumdepot_size = float(row_planet['depot_uranium'])
        planet_name = row_planet['name']
        typeid = row_planet['planet_type']
        bonusid = row_planet['bonus']
        booster = row_planet['boost_percentage']

        # Extract necessary values from user row
        coalbooster_level = row_user['r_coalbooster']
        orebooster_level = row_user['r_orebooster']
        copperbooster_level = row_user['r_copperbooster']
        uraniumbooster_level = row_user['r_uraniumbooster']
        enlargebunker_level = row_user['r_enlargebunker']

        shieldprotection_busy = int(row_planet["shieldprotection_busy"].timestamp()) if row_planet["shieldprotection_busy"] else 0
        shieldcharge_busy = int(row_planet["shieldcharge_busy"].timestamp()) if row_planet["shieldcharge_busy"] else 0
        shieldcharged = row_planet["shieldcharged"]

        # Fetch planet type details
        query_planettype = "SELECT * FROM planettypes WHERE type_id = %s"
        cursor.execute(query_planettype, (typeid,))
        row_planettype = cursor.fetchone()
        type_name = row_planettype['type'] if row_planettype['type'] != "earth" else "atmosphere"

        # Fetch bonus details
        query_bonus = "SELECT * FROM planetlevels WHERE rarity = %s"
        cursor.execute(query_bonus, (bonusid,))
        row_bonus = cursor.fetchone()
        bonus_rate = row_bonus['p_bonus_percentage']
        bonus_name = row_bonus['name']

        # Fetch booster details
        booster_name = None
        if booster is not None:
            query_booster = "SELECT * FROM shop WHERE boost_percentage = %s"
            cursor.execute(query_booster, (booster,))
            row_booster = cursor.fetchone()
            if row_booster:
                booster_name = row_booster['name']

        # Fetch productivity details
        query_productivity = "SELECT * FROM productivity WHERE name = %s AND level = %s"
        coal_production, ore_production, copper_production, uranium_production = 0, 0, 0, 0

        for name, level in [('coalmine', coalmine_level), ('oremine', oremine_level), ('coppermine', coppermine_level), ('uraniummine', uraniummine_level)]:
            cursor.execute(query_productivity, (name, level))
            row_productivity = cursor.fetchone()
            if row_productivity:
                if name == 'coalmine':
                    coal_production = float(row_productivity['coal'] or 0)
                elif name == 'oremine':
                    ore_production = float(row_productivity['ore'] or 0)
                elif name == 'coppermine':
                    copper_production = float(row_productivity['copper'] or 0)
                elif name == 'uraniummine':
                    uranium_production = float(row_productivity['uranium'] or 0)

        # Calculate safe levels
        bunker_protection = (bunker_level * 0.005 + 0.05 + enlargebunker_level * 0.0025)
        coalsafe = coaldepot_size * bunker_protection
        oresafe = oredepot_size * bunker_protection
        coppersafe = copperdepot_size * bunker_protection
        uraniumsafe = uraniumdepot_size * bunker_protection

        # Apply booster if available
        if booster:
            booster *= 2

        # Prepare JSON response
        production_data = {
            "coal": {
                "level": coalmine_level,
                "depot": coaldepot_size,
                "booster": coalbooster_level,
                "production": coal_production,
                "safe": coalsafe
            },
            "ore": {
                "level": oremine_level,
                "depot": oredepot_size,
                "booster": orebooster_level,
                "production": ore_production,
                "safe": oresafe
            },
            "copper": {
                "level": coppermine_level,
                "depot": copperdepot_size,
                "booster": copperbooster_level,
                "production": copper_production,
                "safe": coppersafe
            },
            "uranium": {
                "level": uraniummine_level,
                "depot": uraniumdepot_size,
                "booster": uraniumbooster_level,
                "production": uranium_production,
                "safe": uraniumsafe
            },
            "misc": {
                "type": type_name,
                "bonus": bonus_name,
                "rate": bonus_rate,
                "rune_name": booster_name,
                "rune": booster,
                "planet_name": planet_name,
                "shieldcharge_busy": shieldcharge_busy,
                "shieldprotection_busy": shieldprotection_busy,
                "shieldcharged": shieldcharged
            }
        }

        return jsonify(production_data)

    except mysql.connector.Error as e:
        print(f"Error fetching production data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()   

@app.route('/loadcost', methods=['GET'])
def loadcost():
    id = request.args.get('level', 0)
    name = request.args.get('name', None)
    planetID = request.args.get('planetID', None)
    busy = request.args.get('busy', None)
    
    if planetID is None or name is None:
        return jsonify([])

    if busy is None:
        busy = name
    else:
        if "mine" in busy:
            busy = busy.replace("mine", "")
        elif "center" in busy:
            busy = busy.replace("center", "")
    
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch planet details
        query_planet = "SELECT * FROM planets WHERE id = %s"
        cursor.execute(query_planet, (planetID,))
        row_planet = cursor.fetchone()
        if row_planet is None:
            return jsonify([])

        # Fetch upgrade costs
        query_upgradecosts = "SELECT * FROM upgradecosts WHERE level = %s AND name = %s"
        cursor.execute(query_upgradecosts, (int(id) + 1, name))
        row_upgradecosts = cursor.fetchone()
        if row_upgradecosts is None:
            return jsonify([])

        # Calculate real time based on shipyard level
        shipyardLevel = row_planet['level_shipyard']
        if "ship" in name:
            realTime = ((100 - shipyardLevel) / 100) * row_upgradecosts["upgrade_time"]
        else:
            realTime = row_upgradecosts["upgrade_time"]

        # Determine if busy time exists
        if busy + "_busy" in row_planet:
            busy_time = row_planet[busy + "_busy"]
        else:
            busy_time = None

        # Prepare JSON response
        return jsonify({
            "coal": float(row_upgradecosts["coal"]),
            "ore": float(row_upgradecosts["ore"]),
            "copper": float(row_upgradecosts["copper"]),
            "uranium": float(row_upgradecosts["uranium"]),
            "time": realTime,
            "base": 0,
            "research": 0,
            "busy": busy_time if busy_time is not None else f"{busy}_busy"
        })

    except mysql.connector.Error as e:
        print(f"Error fetching upgrade cost data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/shipyard', methods=['GET'])
def shipyard():
    name = request.args.get('name', None)
    planetid = request.args.get('id', None)
    
    if planetid is None:
        return jsonify([])

    ship_list = []
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch planet details
        query_planet = "SELECT * FROM planets WHERE id = %s"
        cursor.execute(query_planet, (planetid,))
        p = cursor.fetchone()
        if p is None:
            return jsonify([])

        blueprints = []
        if p["blueprints"] is not None:
            blueprints = p["blueprints"].split(",")

        # Fetch ship stats
        query_shipstats = "SELECT * FROM shipstats"
        if name is not None:
            query_shipstats += " WHERE name = %s"
            cursor.execute(query_shipstats, (name,))
        else:
            cursor.execute(query_shipstats)

        shipstats_list = cursor.fetchall()

        for row in shipstats_list:
            ship_class = row["class"]
            upgrade_cost_name = row["name"]
            if upgrade_cost_name[-1] in ["1", "2"] and row["class"] not in ["Explorer", "Transporter", "Yamato"]:
                upgrade_cost_name = upgrade_cost_name[:-1]

            # Fetch upgrade costs
            query_upgradecosts = "SELECT * FROM upgradecosts WHERE name = %s"
            cursor.execute(query_upgradecosts, (upgrade_cost_name,))
            ro = cursor.fetchone()
            if ro is None:
                continue

            coal = ro['coal']
            copper = ro['copper']
            ore = ro['ore']
            uranium = ro['uranium']
            stardust = int(ro['stardust']) if ro['stardust'] is not None else 0
            upgrade_time = ro['upgrade_time'] * (1 - 0.01 * p['level_shipyard'])

            # Fetch user details
            query_users = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query_users, (p['user'],))
            rd = cursor.fetchone()
            if rd is None:
                continue

            cur_skill = rd['r_' + ship_class]
            battlespeed_buff = rd["b_battlespeed"]
            level_shipyardskill = rd['r_shipyard']

            # Fetch ship status
            query_ships = "SELECT * FROM ships WHERE type = %s AND cords_ver = %s AND cords_hor = %s ORDER BY busy_until DESC"
            cursor.execute(query_ships, (row["name"], p['cords_ver'], p['cords_hor']))
            ro = cursor.fetchone()
            busy_time = int(ro['busy_until'].timestamp()) if ro is not None else None

            activated = True if row["name"] in blueprints else False
            apply_battlespeed = battlespeed_buff > datetime.utcnow() if battlespeed_buff is not None else False
            base_speed = row["speed"]
            if apply_battlespeed:
                row["speed"] = row["battlespeed"]

            cost = {"coal": coal, "ore": ore, "copper": copper, "uranium": uranium, "time": upgrade_time, "stardust": stardust}
            ship_list.append({
                "speed": row["speed"],
                "consumption": float(row["consumption"]),
                "longname": row["longname"],
                "class": row["class"],
                "variant": row["variant"],
                "type": row["name"],
                "activated": activated,
                "variant_name": row["variant_name"],
                "structure": row["structure"],
                "armor": row["armor"],
                "shield": row['shield'],
                "rocket": row['rocket'],
                "bullet": row['bullet'],
                "laser": row['laser'],
                "capacity": row["capacity"],
                "busy_until": busy_time,
                "skill": cur_skill,
                "min_level": row['shipyard_level'],
                "cur_level": p['level_shipyard'],
                "cur_level_skill": level_shipyardskill,
                "cost": cost,
                "blueprint": row["blueprint"],
                "basespeed": base_speed,
                "battlespeed": row["battlespeed"],
                "order": row["order"]
            })

        if len(ship_list) == 1:
            return jsonify(ship_list[0])

        return jsonify(ship_list)

    except mysql.connector.Error as e:
        print(f"Error fetching shipyard data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadgift', methods=['GET'])
def loadgift():
    user = request.args.get('user', None)
    if user is None:
        return jsonify([])

    stack = []
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    try:
        cursor = connection.cursor(dictionary=True)

        # Fetch items gifted to the user
        query_items = """
            SELECT items.*, shop.name AS item_name
            FROM items
            LEFT JOIN shop ON items.itemid = shop.itemid
            WHERE items.owner = %s 
              AND items.last_owner IS NOT NULL 
              AND items.activated_trx IS NULL
            ORDER BY items.item_gifted_at
        """
        cursor.execute(query_items, (user,))
        rows = cursor.fetchall()

        for row in rows:
            name = row["item_name"]
            fromm = row['last_owner']
            time = int(row['item_gifted_at'].timestamp())
            stack.append({"name": name, "from": fromm, "time": time})

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching gifted items: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadbattle', methods=['GET'])
def loadbattle():
    mission_id = request.args.get('mission_id', None)
    battle_number = request.args.get('battle_number', None)
    limit = request.args.get('limit', 100)

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    stack = []
    missions = []

    try:
        cursor = connection.cursor(dictionary=True)
        if mission_id is not None:
            if battle_number is None:
                query = "SELECT * FROM battleresults WHERE mission_id = %s"
                cursor.execute(query, (mission_id,))
            else:
                query = "SELECT * FROM battleresults WHERE mission_id = %s AND battle_number = %s"
                cursor.execute(query, (mission_id, battle_number))
        else:
            try:
                limit = int(limit)
            except:
                limit = 100
            query = "SELECT * FROM battleresults ORDER BY date DESC LIMIT %s"
            cursor.execute(query, (limit,))

        rows = cursor.fetchall()

        for row in rows:
            row["date"] = int(row['date'].timestamp())
            final_attacker_ships = json.loads(row["final_attacker_ships"])
            final_defender_ships = json.loads(row["final_defender_ships"])
            initial_attacker_ships = json.loads(row["initial_attacker_ships"])
            initial_defender_ships = json.loads(row["initial_defender_ships"])

            row["initial_attacker_ships"] = [
                {"pos": pos, **initial_attacker_ships[pos]} for pos in initial_attacker_ships
            ]
            row["initial_defender_ships"] = [
                {"pos": pos, **initial_defender_ships[pos]} for pos in initial_defender_ships
            ]
            row["final_attacker_ships"] = [
                {"pos": pos, **final_attacker_ships[pos]} for pos in final_attacker_ships
            ]
            row["final_defender_ships"] = [
                {"pos": pos, **final_defender_ships[pos]} for pos in final_defender_ships
            ]

            stack.append(row)

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching battle results: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


@app.route('/loadranking', methods=['GET'])
def loadranking():
    sort = request.args.get('sort', 'meta')
    limit = request.args.get('limit', 150)
    page = request.args.get('page', None)
    
    if limit is not None:
        try:
            limit = int(limit)
        except:
            limit = 150
     
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    stack = []
    cursor = connection.cursor(dictionary=True)
    
    try:
        if sort == "meta":
            order_by = 'meta_rate DESC'
        elif sort == "meta_rate":
            order_by = 'meta_rate DESC'
        elif sort == "destroyed":
            order_by = 'destroyed_ships_uranium DESC'
        elif sort == "destroyed_ships_uranium":
            order_by = 'destroyed_ships_uranium DESC'
        elif sort == "explorations":
            order_by = 'explorations DESC'
        elif sort == "planets":
            order_by = 'planets DESC'
        elif sort == "fleet":
            order_by = 'ships DESC'
        elif sort == "meta_skill":
            order_by = 'meta_skill DESC'
        else:
            order_by = 'meta_rate DESC'  # Default sorting by meta_rate
        
        query = f"SELECT * FROM ranking ORDER BY {order_by}"
        
        if limit is not None:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            stack.append({
                "user": row["user"],
                "coal": row["rate_coal"],
                "ore": row["rate_ore"],
                "copper": row["rate_copper"],
                "uranium": row["rate_uranium"],
                "meta_rate": row["meta_rate"],
                "meta_skill": row["meta_skill"],
                "explorations": row["explorations"],
                "planets": row["planets"],
                "ships": row["ships"],
                "destroyed_ships": row["destroyed_ships"],
                "destroyed_ships_uranium": float(row["destroyed_ships_uranium"])
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching ranking: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadtranslation', methods=['GET'])
def loadtranslation():
    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    stack = []
    cursor = connection.cursor(dictionary=True)
    
    try:
        query = "SELECT variable, translation FROM translate"
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            stack.append({
                "variable": row["variable"],
                "translation": row["translation"]
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching translations: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadshop', methods=['GET'])
def loadshop():
    user = request.args.get('user', None)

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    stack = []
    cursor = connection.cursor(dictionary=True)
    
    try:
        query = "SELECT * FROM shop ORDER BY id"
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            table = db.cursor(dictionary=True)
            left = 0
            query = "SELECT COUNT(*) as count FROM items WHERE itemid = %s AND date >= %s"
            table.execute(query, (row["itemid"], datetime.utcnow() - timedelta(days=1)))
            left_result = table.fetchone()
            if left_result:
                left = left_result["count"]

            leftt = None
            if row["sales_per_day"] > 0:
                leftt = row["sales_per_day"] - left

            max_left = None
            if row["max_supply"] is not None:
                query = "SELECT COUNT(*) as count FROM items WHERE itemid = %s"
                table.execute(query, (row["itemid"],))
                mleft_result = table.fetchone()
                if mleft_result:
                    mleft = mleft_result["count"]
                    max_left = row["max_supply"] - mleft

            activated_list = []
            if user is not None:
                if row['blueprint'] is not None:
                    query = "SELECT id FROM planets WHERE user = %s AND FIND_IN_SET(%s, blueprints)"
                    table.execute(query, (user, row['blueprint']))
                    planets = table.fetchall()
                    for planet in planets:
                        activated_list.append(planet["id"])
                elif row['boost_percentage'] is not None:
                    query = "SELECT id FROM planets WHERE user = %s AND boost_percentage = %s"
                    table.execute(query, (user, row['boost_percentage']))
                    planets = table.fetchall()
                    for planet in planets:
                        activated_list.append(planet["id"])

            sales_per_day = row["sales_per_day"]
            if sales_per_day < 0:
                sales_per_day = row["max_supply"]
                leftt = max_left

            stack.append({
                'name': row["name"],
                'id': row["itemid"],
                'imgid': row["itemid"],
                'ore': row["ore"],
                'coal': row["coal"],
                'copper': row["copper"],
                'uranium': row["uranium"],
                'booster': row["boost_percentage"],
                'total': sales_per_day,
                'max_supply': row["max_supply"],
                'blueprint': row['blueprint'],
                'activated_planets': activated_list,
                'max_left': max_left,
                'left': leftt,
                "buyable": row["buyable"],
                'cost': row["price"]
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching shop items: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadfleet', methods=['GET'])
def loadfleet():
    planetid = request.args.get('planetid', None)
    user = request.args.get('user', None)

    if user is None or planetid is None:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)
    stack = []

    try:
        # Retrieve planet coordinates
        query = "SELECT cords_hor, cords_ver FROM planets WHERE id = %s"
        cursor.execute(query, (planetid,))
        row = cursor.fetchone()

        if row is None:
            return jsonify([])

        hor = row["cords_hor"]
        ver = row["cords_ver"]

        # Retrieve active support missions for the user
        query = "SELECT mission_id FROM missions WHERE user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s AND mission_type IN ('support', 'upgradeyamato') AND cancel_trx IS NULL"
        cursor.execute(query, (user, hor, ver))
        active_support_missions = [row["mission_id"] for row in cursor.fetchall()]

        # Retrieve ship types information
        query = "SELECT * FROM shipstats"
        cursor.execute(query)
        ship_types = {ro["name"]: ro for ro in cursor.fetchall()}

        # Retrieve user data
        query = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query, (user,))
        userdata = cursor.fetchone()

        if userdata is None:
            return jsonify([])

        battlespeed_buff = userdata["b_battlespeed"]
        apply_battlespeed = False

        if battlespeed_buff is not None and battlespeed_buff > datetime.utcnow():
            apply_battlespeed = True

        # Retrieve ships data
        query = "SELECT * FROM ships WHERE user = %s AND cords_hor = %s AND cords_ver = %s AND busy_until < %s"
        cursor.execute(query, (user, hor, ver, datetime.utcnow()))

        for row in cursor.fetchall():
            ro = ship_types.get(row["type"])

            if ro is None:
                continue

            speed = float(ro["speed"])
            if apply_battlespeed:
                speed = float(ro["battlespeed"])

            stack.append({
                'id': row["id"],
                'type': row["type"],
                'hor': row["cords_hor"],
                'ver': row["cords_ver"],
                'busy': int(row["busy_until"].timestamp()),
                'lastupdate': int(row["last_update"].timestamp()),
                'ore': float(row["qyt_ore"]),
                'uranium': float(row["qyt_uranium"]),
                'copper': float(row["qyt_copper"]),
                'coal': float(row["qyt_coal"]),
                'speed': speed,
                'cons': float(ro["consumption"]),
                'longname': ro["longname"],
                'capacity': float(ro["capacity"]),
                'for_sale': row["for_sale"],
                'order': ro["order"]
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching fleet data: {e}")
        return jsonify({"error": "Internal server error"}), 500

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/planetfleet', methods=['GET'])
def planetfleet():
    planet = request.args.get('planet', None)
    user = request.args.get('user', None)

    if user is None or planet is None:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)
    stack = []

    try:
        # Retrieve user battlespeed buff status
        query = "SELECT b_battlespeed FROM users WHERE username = %s"
        cursor.execute(query, (user,))
        userdata = cursor.fetchone()

        if userdata is None:
            return jsonify([])

        battlespeed_buff = userdata["b_battlespeed"]
        apply_battlespeed = False

        if battlespeed_buff is not None and battlespeed_buff > datetime.utcnow():
            apply_battlespeed = True

        # Construct SQL query to fetch fleet information
        sql = """SELECT t1.*, t2.quantity, t2.for_sale
                 FROM shipstats t1
                 INNER JOIN (
                     SELECT type, COUNT(id) AS quantity, SUM(for_sale) AS for_sale
                     FROM ships
                     WHERE cords_hor IN (SELECT cords_hor FROM planets WHERE id = %s)
                       AND cords_ver IN (SELECT cords_ver FROM planets WHERE id = %s)
                       AND user = %s
                       AND busy_until < CURRENT_TIMESTAMP
                       AND (mission_busy_until IS NULL OR mission_busy_until < CURRENT_TIMESTAMP)
                       AND (mission_id IS NULL OR mission_id NOT IN (
                           SELECT mission_id
                           FROM missions
                           WHERE user = %s
                             AND cords_hor_dest IN (SELECT cords_hor FROM planets WHERE id = %s)
                             AND cords_ver_dest IN (SELECT cords_ver FROM planets WHERE id = %s)
                             AND mission_type IN ('support', 'upgradeyamato')
                             AND cancel_trx IS NULL
                             AND mission_busy_until > CURRENT_TIMESTAMP
                       ))
                     GROUP BY type
                 ) t2 ON t1.name = t2.type"""

        cursor.execute(sql, (planet, planet, user, user, planet, planet))

        # Fetch and process query results
        for row in cursor.fetchall():
            if apply_battlespeed:
                row["speed"] = row["battlespeed"]

            stack.append({
                "type": row["type"],
                "speed": float(row["speed"]),
                "consumption": float(row["consumption"]),
                "longname": row["longname"],
                "capacity": int(row["capacity"]),
                "for_sale": int(row["for_sale"]),
                "class": row["class"],
                "variant": row["variant_name"],
                "structure": int(row["structure"]),
                "armor": int(row["armor"]),
                "rocket": int(row["rocket"]),
                "bullet": int(row["bullet"]),
                "laser": int(row["laser"]),
                "shipyard_level": int(row["shipyard_level"]),
                "quantity": int(row["quantity"]),
                "order": int(row["order"])
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching planet fleet data: {e}")
        return jsonify({"error": "Internal server error"}), 500

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/planetships', methods=['GET'])
def planetships():
    planet = request.args.get('planet', None)
    user = request.args.get('user', None)
    ship_type = request.args.get('type', None)
    limit = request.args.get('limit', None)

    if user is None or planet is None:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)
    stack = []

    try:
        # Retrieve user battlespeed buff status
        query = "SELECT b_battlespeed FROM users WHERE username = %s"
        cursor.execute(query, (user,))
        userdata = cursor.fetchone()

        if userdata is None:
            return jsonify([])

        battlespeed_buff = userdata["b_battlespeed"]
        apply_battlespeed = False

        if battlespeed_buff is not None and battlespeed_buff > datetime.utcnow():
            apply_battlespeed = True

        # Construct SQL query to fetch planet ships information
        type_filter = "1 = 1"
        if ship_type is not None:
            type_filter = f"type = '{ship_type}'"

        limit_filter = ""
        if limit is not None:
            limit_filter = f"LIMIT {limit}"

        sql = f"""SELECT t1.*, t2.*
                  FROM shipstats t1
                  INNER JOIN (
                      SELECT *
                      FROM ships
                      WHERE cords_hor IN (SELECT cords_hor FROM planets WHERE id = '{planet}')
                        AND cords_ver IN (SELECT cords_ver FROM planets WHERE id = '{planet}')
                        AND user = '{user}'
                        AND {type_filter}
                        AND busy_until < CURRENT_TIMESTAMP
                        AND (mission_busy_until IS NULL OR mission_busy_until < CURRENT_TIMESTAMP)
                        AND (mission_id IS NULL OR mission_id NOT IN (
                            SELECT mission_id
                            FROM missions
                            WHERE user = '{user}'
                              AND cords_hor_dest IN (SELECT cords_hor FROM planets WHERE id = '{planet}')
                              AND cords_ver_dest IN (SELECT cords_ver FROM planets WHERE id = '{planet}')
                              AND mission_type IN ('support', 'upgradeyamato')
                              AND cancel_trx IS NULL
                              AND mission_busy_until > CURRENT_TIMESTAMP
                        ))
                  ) t2 ON t1.name = t2.type
                  {limit_filter}
               """

        cursor.execute(sql)

        # Fetch and process query results
        stack = []
        for row in cursor.fetchall():
            if apply_battlespeed:
                row["speed"] = row["battlespeed"]

            stack.append({
                "id": row["id"],
                "type": row["type"],
                "speed": float(row["speed"]),
                "consumption": float(row["consumption"]),
                "longname": row["longname"],
                "capacity": int(row["capacity"]),
                "for_sale": int(row["for_sale"]),
                "class": row["class"],
                "variant": row["variant_name"],
                "structure": int(row["structure"]),
                "armor": int(row["armor"]),
                "rocket": int(row["rocket"]),
                "bullet": int(row["bullet"]),
                "laser": int(row["laser"]),
                "shipyard_level": int(row["shipyard_level"]),
                "order": int(row["order"]),
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching planet ships data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadcorddata', methods=['GET'])
def loadcorddata():
    x = request.args.get('x', None)
    y = request.args.get('y', None)
    
    if x is None or y is None:
        return jsonify({"error": "Coordinates not provided"}), 400

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)

    try:
        type = "nothing"
        user = None
        name = None

        # Check if there's a planet at the specified coordinates
        query = "SELECT * FROM planets WHERE cords_hor = %s AND cords_ver = %s"
        cursor.execute(query, (x, y))
        row = cursor.fetchone()

        if row is not None:
            type = "planet"
            user = row['user']
            name = row['name']
        else:
            # Check if there's space exploration data at the specified coordinates
            query = "SELECT * FROM space WHERE c_hor = %s AND c_ver = %s"
            cursor.execute(query, (x, y))
            ro = cursor.fetchone()

            if ro is not None:
                type = "explored"

        return jsonify({"type": type, "user": user, "name": name})

    except mysql.connector.Error as e:
        print(f"Error fetching coordinate data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loaditems', methods=['GET'])
def loaditems():
    user = request.args.get('user', None)
    if user is None:
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)

    try:
        stack = []

        # Fetch items data for the user from MySQL database
        query = """
                SELECT i.itemid, i.uid, s.name, s.ore, s.uranium, s.copper, s.coal, s.boost_percentage, s.blueprint, i.for_sale,
                       COUNT(i.itemid) as total
                FROM items i
                INNER JOIN shop s ON i.itemid = s.itemid
                WHERE i.owner = %s AND i.activated_trx IS NULL
                GROUP BY i.itemid
                """
        cursor.execute(query, (user,))
        rows = cursor.fetchall()

        for row in rows:
            stack.append({
                'id': row["itemid"],
                'imgid': row["itemid"],  # Assuming imgid is the same as itemid
                'uid': row["uid"],
                'name': row["name"],
                'total': row["total"],
                'ore': row["ore"],
                'uranium': row["uranium"],
                'copper': row["copper"],
                'coal': row["coal"],
                'booster': row["boost_percentage"],
                'blueprint': row["blueprint"],
                'for_sale': row["for_sale"]
            })

        return jsonify(stack)

    except mysql.connector.Error as e:
        print(f"Error fetching items data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadgalaxy', methods=['GET'])
def loadgalaxy():
    x = request.args.get('x', None)
    y = request.args.get('y', None)
    width = request.args.get('width', 23)
    height = request.args.get('height', 14)
    user = request.args.get('user', None)
    
    if x is None or x == 'null' or x == 'undefined':
        return jsonify([])
    if y is None or y == 'null' or y == 'undefined':
        return jsonify([])

    try:
        width = int(width)
    except:
        width = 23
    try:
        height = int(height)
    except:
        height = 14

    if width > 125:
        width = 125
    if height > 125:
        height = 125

    try:
        xoffsetleft = int((width ) / 2)
        xoffsetright = (width ) - int((width) / 2)
        yoffsetup = int((height )/ 2)
        yoffsetdown = (height) - int((height) / 2)
        
        xmin = int(float(x)) - xoffsetleft
        xmax = int(float(x)) + xoffsetright
        ymin = int(float(y)) - yoffsetdown
        ymax = int(float(y)) + yoffsetup
    except Exception as e:
        print(f"Error in calculating coordinates: {e}")
        return jsonify([])

    connection = get_db_connection()
    if connection is None:
        return jsonify({"error": "Database connection error"}), 500

    cursor = connection.cursor(dictionary=True)

    try:
        explored = []
        explore = []
        planets = []

        # Fetch explored space data
        query = """
                SELECT c_hor, c_ver, type, user, UNIX_TIMESTAMP(date) AS date
                FROM space
                WHERE c_hor BETWEEN %s AND %s
                  AND c_ver BETWEEN %s AND %s
                """
        cursor.execute(query, (xmin, xmax, ymin, ymax))
        explored_rows = cursor.fetchall()

        for row in explored_rows:
            explored.append({
                'x': row["c_hor"],
                'y': row["c_ver"],
                'type': 'explored',
                'user': row['user'],
                'date': int(row['date'])
            })

        # Fetch active missions
        query = """
                SELECT cords_hor_dest, cords_ver_dest, cords_hor, cords_ver, mission_type, user, ships,
                       UNIX_TIMESTAMP(busy_until) AS date, UNIX_TIMESTAMP(busy_until_return) AS date_return
                FROM missions
                WHERE cords_hor_dest BETWEEN %s AND %s
                  AND cords_ver_dest BETWEEN %s AND %s
                  AND busy_until_return > NOW()
                """
        cursor.execute(query, (xmin, xmax, ymin, ymax))
        active_missions = cursor.fetchall()

        for row in active_missions:
            mission_type = row['mission_type']
            if mission_type == "explorespace":
                mission_type = "explore"
            explore.append({
                'x': row["cords_hor_dest"],
                'y': row["cords_ver_dest"],
                'start_x': row["cords_hor"],
                'start_y': row["cords_ver"],
                'type': mission_type,
                'user': row['user'],
                'ships': row['ships'],
                'date': int(row['date']),
                'date_return': int(row['date_return']) if row['date_return'] else None
            })

        # Fetch planets data
        query = """
                SELECT cords_hor, cords_ver, id, img_id, abandoned, user
                FROM planets
                WHERE cords_hor BETWEEN %s AND %s
                  AND cords_ver BETWEEN %s AND %s
                """
        cursor.execute(query, (xmin, xmax, ymin, ymax))
        planets_rows = cursor.fetchall()

        for row in planets_rows:
            tx = row["cords_hor"]
            ty = row["cords_ver"]

            query = """
                    SELECT name
                    FROM planetlevels
                    WHERE rarity = %s
                    """
            cursor.execute(query, (row["bonus"],))
            rarity_row = cursor.fetchone()
            rarity = rarity_row["name"] if rarity_row else ""

            query = """
                    SELECT type
                    FROM planettypes
                    WHERE type_id = %s
                    """
            cursor.execute(query, (row["planet_type"],))
            type_row = cursor.fetchone()
            type = type_row["type"] if type_row else ""

            img = GetPlanetImg(rarity, type, row['img_id'])  # Assuming GetPlanetImg function is defined elsewhere
            planets.append({
                'x': tx,
                'y': ty,
                'type': 'planet',
                'id': row['id'],
                'img': img,
                'abandoned': row["abandoned"],
                'user': row["user"]
            })

        area = {'xmin': xmin, 'xmax': xmax, 'ymin': ymin, 'ymax': ymax}

        return jsonify({'explored': explored, 'explore': explore, 'planets': planets, 'area': area})

    except mysql.connector.Error as e:
        print(f"Error fetching galaxy data: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route('/loadfleetmission', methods=['GET'])
def loadfleetmission():
    planetid = request.args.get('planetid', None)
    user = request.args.get('user', None)
    active = request.args.get('active', None)
    outgoing = request.args.get('outgoing', None)
    onlyuser = request.args.get('onlyuser', None)
    hold = request.args.get('hold', None)
    limit = request.args.get('limit', None)
    page = request.args.get('page', None)

    if user is None:
        return jsonify([])

    if active is not None:
        try:
            active = int(active)
        except ValueError:
            return jsonify([])

    if hold is not None:
        try:
            hold = int(hold)
        except ValueError:
            return jsonify([])

    if onlyuser is not None:
        try:
            onlyuser = int(onlyuser)
        except ValueError:
            return jsonify([])

    if outgoing is not None:
        try:
            outgoing = int(outgoing)
        except ValueError:
            return jsonify([])

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            limit = None

    if page is not None:
        try:
            page = int(page)
        except ValueError:
            page = 0
    else:
        page = 0

    try:
        connection = get_db_connection()

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            planet_list = []
            if planetid is None and user is not None:
                query = "SELECT * FROM planets WHERE user = %s"
                cursor.execute(query, (user,))
                planet_list = cursor.fetchall()
            elif planetid is not None:
                query = "SELECT * FROM planets WHERE id = %s"
                cursor.execute(query, (planetid,))
                planet = cursor.fetchone()
                if planet is None:
                    return jsonify([])  # Return empty list if planet not found
                planet_list.append(planet)

            stack = []

            for planet_row in planet_list:
                posx = planet_row["cords_hor"]
                posy = planet_row["cords_ver"]

                missions_list = []

                if onlyuser == 1 and user is not None:
                    if active == 1:
                        query = ("SELECT * FROM missions WHERE (user = %s AND cords_hor = %s AND cords_ver = %s "
                                 "AND busy_until_return IS NULL AND busy_until > %s) "
                                 "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                 "AND busy_until_return IS NULL AND busy_until > %s) "
                                 "OR (user = %s AND cords_hor = %s AND cords_ver = %s "
                                 "AND busy_until_return > %s AND busy_until > %s) "
                                 "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                 "AND busy_until_return > %s AND busy_until > %s) "
                                 "ORDER BY busy_until")

                        current_time = datetime.utcnow()
                        cursor.execute(query, (user, posx, posy, current_time,
                                               user, posx, posy, current_time,
                                               user, posx, posy, current_time, current_time,
                                               user, posx, posy, current_time, current_time))
                        missions_list += cursor.fetchall()

                    elif active == 0:
                        if limit is None:
                            query = ("SELECT * FROM missions WHERE (user = %s AND cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return IS NULL) "
                                     "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return IS NULL) "
                                     "OR (user = %s AND cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return < %s) "
                                     "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return < %s) "
                                     "ORDER BY busy_until")

                            current_time = datetime.utcnow()
                            cursor.execute(query, (user, posx, posy,
                                                   user, posx, posy,
                                                   user, posx, posy, current_time,
                                                   user, posx, posy, current_time))
                            missions_list += cursor.fetchall()

                        elif limit is not None:
                            query = ("SELECT * FROM missions WHERE (user = %s AND cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return IS NULL) "
                                     "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return IS NULL) "
                                     "OR (user = %s AND cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return < %s) "
                                     "OR (user = %s AND cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return < %s) "
                                     "ORDER BY busy_until LIMIT %s OFFSET %s")

                            current_time = datetime.utcnow()
                            cursor.execute(query, (user, posx, posy,
                                                   user, posx, posy,
                                                   user, posx, posy, current_time,
                                                   user, posx, posy, current_time,
                                                   limit, limit * page))
                            missions_list += cursor.fetchall()

                else:
                    if active == 1:
                        query = ("SELECT * FROM missions WHERE (cords_hor = %s AND cords_ver = %s "
                                 "AND busy_until_return IS NULL) "
                                 "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                 "AND busy_until_return IS NULL) "
                                 "OR (cords_hor = %s AND cords_ver = %s "
                                 "AND busy_until_return > %s AND busy_until > %s) "
                                 "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                 "AND busy_until_return > %s AND busy_until > %s) "
                                 "ORDER BY busy_until DESC")

                        current_time = datetime.utcnow()
                        cursor.execute(query, (posx, posy,
                                               posx, posy,
                                               posx, posy, current_time, current_time,
                                               posx, posy, current_time, current_time))
                        missions_list += cursor.fetchall()

                    elif active == 0:
                        if limit is None:
                            query = ("SELECT * FROM missions WHERE (cords_hor = %s AND cords_ver = %s "
                                     "AND mission_type = 'explorespace' AND busy_until_return IS NULL) "
                                     "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND mission_type = 'explorespace' AND busy_until_return IS NULL) "
                                     "OR (cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return < %s) "
                                     "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return < %s) "
                                     "ORDER BY busy_until")

                            current_time = datetime.utcnow()
                            cursor.execute(query, (posx, posy,
                                                   posx, posy,
                                                   posx, posy, current_time,
                                                   posx, posy, current_time))
                            missions_list += cursor.fetchall()

                        elif limit is not None:
                            query = ("SELECT * FROM missions WHERE (cords_hor = %s AND cords_ver = %s "
                                     "AND mission_type = 'explorespace' AND busy_until_return IS NULL) "
                                     "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND mission_type = 'explorespace' AND busy_until_return IS NULL) "
                                     "OR (cords_hor = %s AND cords_ver = %s "
                                     "AND busy_until_return < %s) "
                                     "OR (cords_hor_dest = %s AND cords_ver_dest = %s "
                                     "AND busy_until_return < %s) "
                                     "ORDER BY busy_until LIMIT %s OFFSET %s")

                            current_time = datetime.utcnow()
                            cursor.execute(query, (posx, posy,
                                                   posx, posy,
                                                   posx, posy, current_time,
                                                   posx, posy, current_time,
                                                   limit, limit * page))
                            missions_list += cursor.fetchall()

                for row in missions_list:
                    id = row['mission_id']
                    mission_user = row['user']

                    if id in [m['id'] for m in stack]:
                        continue

                    type = row['mission_type']
                    arrival = int(row['busy_until'].timestamp()) if row['busy_until'] else None
                    start_date = int(row['start_date'].timestamp()) if row['start_date'] else None
                    arrival_return = int(row['busy_until_return'].timestamp()) if row['busy_until_return'] else None

                    try:
                        if type == 'mission':
                            busy_until = int(row['busy_until'].timestamp()) if row['busy_until'] else None
                            stack.append({
                                'id': id,
                                'start_date': start_date,
                                'arrival': arrival,
                                'arrival_return': arrival_return,
                                'busy_until': busy_until,
                                'type': 'missions'
                            })

                        elif type == 'explorespace':
                            busy_until = int(row['busy_until'].timestamp()) if row['busy_until'] else None
                            stack.append({
                                'id': id,
                                'start_date': start_date,
                                'arrival': arrival,
                                'arrival_return': arrival_return,
                                'busy_until': busy_until,
                                'type': 'explorespace'
                            })

                        elif type == 'siege':
                            stack.append({
                                'id': id,
                                'start_date': start_date,
                                'arrival': arrival,
                                'arrival_return': arrival_return,
                                'type': 'siege'
                            })

                        elif type == 'collonization':
                            stack.append({
                                'id': id,
                                'start_date': start_date,
                                'arrival': arrival,
                                'arrival_return': arrival_return,
                                'type': 'collonization'
                            })

                    except Exception as e:
                        print(f"Error in processing missions: {e}")

        else:
            return jsonify([])

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return jsonify([])

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

    return jsonify(stack)

@app.route('/loadplanet', methods=['GET'])
def loadplanet():
    id = request.args.get('id', None)
    x = request.args.get('x', None)
    y = request.args.get('y', None)

    if not id  and (not x  or not y):
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            if id is not None:
                cursor.execute(f"SELECT * FROM planets WHERE id = {id}")
            else:
                cursor.execute(f"SELECT * FROM planets WHERE cords_hor = {x} AND cords_ver = {y}")

            row = cursor.fetchone()

            if row is None:
                return jsonify([])

            planet_id = row["id"]
            cursor.execute(f"SELECT * FROM planetlevels WHERE rarity = '{row['bonus']}'")
            rows = cursor.fetchone()
            bonus = rows["p_bonus_percentage"]
            rarity = rows["name"]

            typenumber = 1 if row['planet_type'] == 0 else row['planet_type']
            cursor.execute(f"SELECT * FROM planettypes WHERE type_id = {typenumber}")
            rows = cursor.fetchone()
            planet_type = rows["type"]

            cursor.execute(f"SELECT COUNT(*) FROM planets WHERE planet_type = {typenumber} AND bonus = '{row['bonus']}'")
            total_type = cursor.fetchone()[0]

            img = GetPlanetImg(rarity, planet_type, row['img_id'])
            cr_date_ts = int(row["date_disc"].timestamp())
            shieldprotection_busy = int(row["shieldprotection_busy"].timestamp()) if row["shieldprotection_busy"] else 0
            shieldcharge_busy = int(row["shieldcharge_busy"].timestamp()) if row["shieldcharge_busy"] else 0
            shieldcharged = row["shieldcharged"]

            return jsonify({
                'planet_name': row['name'],
                'planet_id': planet_id,
                'total_type': total_type,
                'user': row["user"],
                'planet_type': planet_type,
                'planet_bonus': bonus,
                'planet_rarity': rarity,
                'planet_corx': row["cords_hor"],
                'planet_cory': row["cords_ver"],
                'planet_crts': cr_date_ts,
                'img': img,
                'level_base': row["level_base"],
                'level_coal': row["level_coal"],
                'level_ore': row['level_ore'],
                'level_copper': row['level_copper'],
                'level_uranium': row['level_uranium'],
                'level_ship': row['level_shipyard'],
                'level_research': row['level_research'],
                'level_coaldepot': row['level_coaldepot'],
                'level_oredepot': row['level_oredepot'],
                'level_copperdepot': row['level_copperdepot'],
                'level_uraniumdepot': row['level_uraniumdepot'],
                'shieldcharge_busy': shieldcharge_busy,
                'shieldprotection_busy': shieldprotection_busy,
                'shieldcharged': shieldcharged,
                'startplanet': row['startplanet'],
                'abandoned': row['abandoned'],
                'for_sale': row['for_sale']
            })

        except Error as e:
            print(f"a Error executing MySQL query: {e}")
            return jsonify([])
        
        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()
    else:
        return jsonify([])
    
@app.route('/transactions', methods=['GET'])
def transactions():
    limit = request.args.get('limit', 150)
    tr_type = request.args.get('type', None)
    user = request.args.get('user', None)

    try:
        limit = int(limit)
    except ValueError:
        limit = 150

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            sql = "SELECT * FROM transactions WHERE virtualop = 0"

            if tr_type:
                sql += f" AND tr_type = '{tr_type}'"
            if user:
                sql += f" AND user = '{user}'"

            sql += " ORDER BY date DESC LIMIT %s"

            cursor.execute(sql, (limit,))
            trx_list = cursor.fetchall()

            for trx in trx_list:
                trx["date"] = int(trx["date"].timestamp())

            return jsonify(trx_list)

        except mysql.connector.Error as e:
            print(f"b Error executing MySQL query: {e}")
            return jsonify([])

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()


    else:
        return jsonify([])

@app.route('/state', methods=['GET'])
def state():
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            sql = "SELECT * FROM status WHERE id = 1"
            cursor.execute(sql)
            status = cursor.fetchone()

            last_steem_block_num = status["last_steem_block_num"]
            if status["tracker_stop_block_num"] > status["last_steem_block_num"]:
                last_steem_block_num = status["tracker_stop_block_num"]

            processing_delay_seconds = (last_steem_block_num - status["first_unprocessed_block_num"]) * 3
            tracker_delay_seconds = (last_steem_block_num - status["latest_block_num"]) * 3

            response = {
                "latest_block_num": last_steem_block_num,
                "first_unprocessed_block_num": status["first_unprocessed_block_num"],
                "tracker_block_num": status["latest_block_num"],
                "processing_delay_seconds": processing_delay_seconds,
                "tracker_delay_seconds": tracker_delay_seconds
            }

            return jsonify(response)

        except mysql.connector.Error as e:
            print(f"c Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/season', methods=['GET'])
def season():
    timestamp = request.args.get('timestamp', None)

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            table_name = "season"
            
            if timestamp is None:
                sql = f"SELECT * FROM {table_name} ORDER BY end_date DESC LIMIT 1"
                cursor.execute(sql)
                season = cursor.fetchone()
            else:
                query_date = datetime.fromtimestamp(int(timestamp))
                sql = f"SELECT * FROM {table_name} WHERE end_date > %s AND start_date < %s"
                cursor.execute(sql, (query_date, query_date))
                season = cursor.fetchone()

            if not season:
                return jsonify({})

            # Convert to appropriate types
            season["steem_rewards"] = float(season["steem_rewards"])
            season["leach_rate"] = float(season["leach_rate"]) if season["leach_rate"] is not None else 0.0
            season["deploy_rate"] = float(season["deploy_rate"]) if season["deploy_rate"] is not None else 0.0
            season["start_date"] = int(season["start_date"].timestamp())
            season["end_date"] = int(season["end_date"].timestamp())

            return jsonify(season)

        except mysql.connector.Error as e:
            print(f"d Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500
    
@app.route('/seasonranking', methods=['GET'])
def seasonranking():
    sort = request.args.get('sort', 'total_reward')
    limit = request.args.get('limit', 150)
    timestamp = request.args.get('timestamp', None)
    
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            limit = 150     
    
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            table_name_season = "season"
            table_name_seasonranking = "seasonranking"
            
            # Fetch current season details based on timestamp or latest end_date
            if not timestamp:
                sql = f"SELECT * FROM {table_name_season} ORDER BY end_date DESC LIMIT 1"
                cursor.execute(sql)
                season = cursor.fetchone()
            else:
                query_date = datetime.fromtimestamp(int(timestamp))
                sql = f"SELECT * FROM {table_name_season} WHERE end_date > %s AND start_date < %s"
                cursor.execute(sql, (query_date, query_date))
                season = cursor.fetchone()

            if not season:
                return jsonify({})

            season_id = season['id']
            
            # Fetch season ranking data based on sort criteria and season_id
            if sort == "total_reward":
                order_by = '-total_reward'
            elif sort == "build_reward":
                order_by = '-build_reward'
            elif sort == "destroy_reward":
                order_by = '-destroy_reward'
            else:
                order_by = '-total_reward'  # Default sort by total_reward
            
            sql = f"SELECT * FROM {table_name_seasonranking} WHERE season_id = %s ORDER BY {order_by} LIMIT %s"
            cursor.execute(sql, (season_id, limit))
            ranking = cursor.fetchall()

            # Convert to appropriate types
            for row in ranking:
                row['build_reward'] = float(row['build_reward'])
                row['destroy_reward'] = float(row['destroy_reward'])
                row['total_reward'] = float(row['total_reward'])
            
            # Convert season data to appropriate types
            season["steem_rewards"] = float(season["steem_rewards"])
            season["leach_rate"] = float(season["leach_rate"]) if season["leach_rate"] is not None else 0.0
            season["deploy_rate"] = float(season["deploy_rate"]) if season["deploy_rate"] is not None else 0.0
            season["start_date"] = int(season["start_date"].timestamp())
            season["end_date"] = int(season["end_date"].timestamp())

            return jsonify({
                "id": season["id"],
                "start_date": season["start_date"],
                "end_date": season["end_date"],
                "steem_rewards": season["steem_rewards"],
                "name": season["name"],
                "leach_rate": season["leach_rate"],
                "deploy_rate": season["deploy_rate"],
                "ranking": ranking
            })

        except mysql.connector.Error as e:
            print(f"e Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500
    
@app.route('/activateditems', methods=['GET'])
def activateditems():
    user = request.args.get('user', None)
    planetid = request.args.get('planetid', None)
    
    if user is None or planetid is None:
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Fetch items activated to the specified planet for the user
            sql = "SELECT * FROM items WHERE owner = %s AND activated_to = %s"
            cursor.execute(sql, (user, planetid))
            items = cursor.fetchall()

            stack = []
            for row in items:
                # Fetch item details from shop table based on itemid
                sql_shop = "SELECT * FROM shop WHERE itemid = %s"
                cursor.execute(sql_shop, (row["itemid"],))
                ro = cursor.fetchone()

                if ro:
                    stack.append({
                        'id': row["itemid"],
                        'imgid': row["itemid"],  # Assuming imgid is the same as itemid
                        'uid': row["uid"],
                        'name': ro["name"],
                        'ore': ro["ore"],
                        'uranium': ro["uranium"],
                        'copper': ro["copper"],
                        'coal': ro["coal"],
                        'booster': ro["boost_percentage"],
                        'blueprint': ro["blueprint"],
                        'activated_date': int(row["activated_date"].timestamp())
                    })

            return jsonify(stack)

        except mysql.connector.Error as e:
            print(f"f Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/missions', methods=['GET'])
def missions():
    limit = request.args.get('limit', 100)
    user = request.args.get('user', None)
    mission_type = request.args.get('mission_type', None)
    cords_hor = request.args.get('cords_hor', None)
    cords_ver = request.args.get('cords_ver', None)
    cords_hor_dest = request.args.get('cords_hor_dest', None)
    cords_ver_dest = request.args.get('cords_ver_dest', None)
    
    if limit is not None:
        try:
            limit = int(limit)
        except:
            limit = 100

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Construct the filter based on query parameters
            filters = {}
            if user:
                filters["user"] = user
            if mission_type:
                filters["mission_type"] = mission_type
            if cords_hor:
                filters["cords_hor"] = cords_hor
            if cords_ver:
                filters["cords_ver"] = cords_ver
            if cords_hor_dest:
                filters["cords_hor_dest"] = cords_hor_dest
            if cords_ver_dest:
                filters["cords_ver_dest"] = cords_ver_dest

            # Fetch missions based on the constructed filter
            sql = "SELECT * FROM missions"
            if filters:
                sql += " WHERE " + " AND ".join(f"{key} = %s" for key in filters)
                values = tuple(filters.values())
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)

            mission_list = []
            for mission in cursor.fetchmany(size=limit):
                mission["busy_until"] = int(mission["busy_until"].timestamp())
                mission["date"] = int(mission["date"].timestamp())
                if mission["busy_until_return"] is not None:
                    mission["busy_until_return"] = int(mission["busy_until_return"].timestamp())
                if mission["ships"] is None:
                    mission["ships"] = {
                        'explorership': mission['n_explorership'],
                        'transportship': mission['n_transportship'],
                        'corvette': mission['n_corvette'],
                        'frigate': mission['n_frigate'],
                        'destroyer': mission['n_destroyer'],
                        'cruiser': mission['n_cruiser'],
                        'battlecruiser': mission['n_battlecruiser'],
                        'carrier': mission['n_carrier'],
                        'dreadnought': mission['n_dreadnought']
                    }

                # Fetch additional details from related tables
                sql_battle_results = "SELECT COUNT(*) as count FROM battleresults WHERE mission_id = %s"
                cursor.execute(sql_battle_results, (mission["mission_id"],))
                battle_number = cursor.fetchone()["count"]

                sql_activity = "SELECT * FROM activity WHERE mission_id = %s"
                cursor.execute(sql_activity, (mission["mission_id"],))
                rows = cursor.fetchone()

                if rows:
                    result = rows["result"]
                    new_planet_id = rows["new_planet_id"]
                    new_item_id = rows["new_item_id"]
                    new_stardust = rows["new_stardust"]
                    if new_stardust is not None:
                        new_stardust = int(new_stardust)
                else:
                    result = None
                    new_planet_id = None
                    new_item_id = None
                    new_stardust = None

                mission_clean = {
                    "busy_until": mission["busy_until"], "busy_until_return": mission["busy_until_return"],
                    "cords_hor": mission["cords_hor"], "cords_ver": mission["cords_ver"],
                    "cords_hor_dest": mission["cords_hor_dest"], "cords_ver_dest": mission["cords_ver_dest"],
                    "date": mission["date"], "mission_id": mission["mission_id"], "mission_type": mission["mission_type"],
                    "qyt_coal": mission["qyt_coal"], "qyt_ore": mission["qyt_ore"], "qyt_copper": mission["qyt_copper"],
                    "ships": mission["ships"], "user": mission["user"], "result": result, "new_planet_id": new_planet_id,
                    "new_item_id": new_item_id, "new_stardust": new_stardust, "battles": battle_number
                }

                mission_list.append(mission_clean)

            return jsonify(mission_list)

        except mysql.connector.Error as e:
            print(f"g Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()
            

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/burnrates', methods=['GET'])
def burnrates():
   return jsonify([{"bonus": "4", "planet_type": "5", "burnrate": 9000000 * 1e8},
                   {"bonus": "4", "planet_type": "4", "burnrate": 7000000 * 1e8},
                   {"bonus": "4", "planet_type": "3", "burnrate": 6000000 * 1e8},
                   {"bonus": "4", "planet_type": "2", "burnrate": 5000000 * 1e8},
                   {"bonus": "4", "planet_type": "1", "burnrate": 3000000 * 1e8},
                   {"bonus": "3", "planet_type": "5", "burnrate": 90000 * 1e8},
                   {"bonus": "3", "planet_type": "4", "burnrate": 70000 * 1e8},
                   {"bonus": "3", "planet_type": "3", "burnrate": 60000 * 1e8},
                   {"bonus": "3", "planet_type": "2", "burnrate": 50000 * 1e8},
                   {"bonus": "3", "planet_type": "1", "burnrate": 30000 * 1e8},
                   {"bonus": "2", "planet_type": "5", "burnrate": 30000 * 1e8},
                   {"bonus": "2", "planet_type": "4", "burnrate": 25000 * 1e8},
                   {"bonus": "2", "planet_type": "3", "burnrate": 20000 * 1e8},
                   {"bonus": "2", "planet_type": "2", "burnrate": 18000 * 1e8},
                   {"bonus": "2", "planet_type": "1", "burnrate": 10000 * 1e8},
                   {"bonus": "1", "planet_type": "5", "burnrate": 15000 * 1e8},
                   {"bonus": "1", "planet_type": "4", "burnrate": 12000 * 1e8},
                   {"bonus": "1", "planet_type": "3", "burnrate": 10000 * 1e8},
                   {"bonus": "1", "planet_type": "2", "burnrate": 9000 * 1e8},
                   {"bonus": "1", "planet_type": "1", "burnrate": 5000 * 1e8},
                   ])


@app.route('/galaxyplanets', methods=['GET'])
def galaxyplanets():
    limit = request.args.get('limit', 100000)
    after = request.args.get('after', None)
    
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            limit = 100000

    if after is not None:
        try:
            queryDate = datetime.fromtimestamp(int(after))
        except ValueError:
            queryDate = datetime.utcnow()

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Construct the filter based on query parameters
            sql = "SELECT * FROM planets"
            if after is not None:
                sql += " WHERE last_update > %s"
                cursor.execute(sql, (queryDate,))
            else:
                cursor.execute(sql)

            planet_list = []
            for planet in cursor.fetchmany(size=limit):
                planet_small = {
                    "x": planet["cords_hor"], "y": planet["cords_ver"], "id": planet["id"], "update": int(planet["last_update"].timestamp()),
                    "user": planet["user"], "bonus": planet["bonus"], "type": planet["planet_type"],
                    "name": planet["name"], "starter": planet["startplanet"], "abandoned": planet["abandoned"], "for_sale": planet["for_sale"], "img_id": planet["img_id"]
                }
                planet_list.append(planet_small)

            return jsonify(planet_list)

        except mysql.connector.Error as e:
            print(f"h Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/asks', methods=['GET'])
def asks():
    limit = request.args.get('limit', 100)
    user = request.args.get('user', None)
    category = request.args.get('category', None)
    subcategory = request.args.get('subcategory', None)
    itype = request.args.get('type', None)
    active = request.args.get('active', None)
    sold = request.args.get('sold', None)
    id = request.args.get('id', None)
    uid = request.args.get('uid', None)
    market = request.args.get('market', None)
    orderby = request.args.get('orderby', 'price')
    order = request.args.get("order", "asc")

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            limit = 100  

    if order == "asc":
        orderkey = ""
    elif order == "desc":
        orderkey = "-"

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Construct the SQL query
            sql = f"SELECT * FROM asks"

            # Build WHERE clause dynamically
            where_conditions = []
            if user is not None:
                where_conditions.append(f"user = '{user}'")
            if category is not None:
                where_conditions.append(f"category = '{category}'")
            if subcategory is not None:
                where_conditions.append(f"subcategory = '{subcategory}'")
            if itype is not None:
                where_conditions.append(f"type = '{itype}'")
            if active == "1":
                where_conditions.append("failed IS NULL AND sold IS NULL AND cancel_trx IS NULL AND buy_trx IS NULL")
            if sold == "1":
                where_conditions.append("sold IS NOT NULL")
            if uid is not None:
                where_conditions.append(f"uid = '{uid}'")
            if id is not None:
                where_conditions.append(f"id = '{id}'")
            if market is not None:
                where_conditions.append(f"market = '{market}'")

            if where_conditions:
                sql += " WHERE " + " AND ".join(where_conditions)

            # Add ORDER BY clause
            sql += f" ORDER BY {orderkey}{orderby} LIMIT {limit}"

            # Execute the query
            cursor.execute(sql)
            ask_list = cursor.fetchall()

            # Process results
            for ask in ask_list:
                ask["date"] = int(ask["date"].timestamp()) if ask["date"] else None
                ask["sold"] = int(ask["sold"].timestamp()) if ask["sold"] else None
                ask["failed"] = int(ask["failed"].timestamp()) if ask["failed"] else None

            return jsonify(ask_list)

        except mysql.connector.Error as e:
            print(f"i Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/lowestasks', methods=['GET'])
def lowestasks():
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Construct SQL query
            sql = """
                SELECT *
                FROM (
                    SELECT *
                    FROM `asks`
                    WHERE cancel_trx IS NULL AND buy_trx IS NULL AND sold IS NULL AND failed IS NULL
                ) t5
                INNER JOIN (
                    SELECT utype utype3, MIN(price) price3, MIN(date) date3
                    FROM (
                        SELECT *
                        FROM (
                            SELECT *
                            FROM `asks`
                            WHERE cancel_trx IS NULL AND buy_trx IS NULL AND sold IS NULL AND failed IS NULL
                        ) t3
                        INNER JOIN (
                            SELECT utype utype2, MIN(price) price2
                            FROM asks
                            WHERE cancel_trx IS NULL AND buy_trx IS NULL AND sold IS NULL AND failed IS NULL
                            GROUP BY utype
                        ) t1 ON t3.utype = t1.utype2 AND t3.price = t1.price2
                    ) t4
                    GROUP BY utype
                ) t6 ON t5.utype = t6.utype3 AND t5.price = t6.price3 AND t5.date = t6.date3
                ORDER BY price ASC
            """

            # Execute the query
            cursor.execute(sql)
            ask_list = cursor.fetchall()

            # Process results
            for ask in ask_list:
                ask["date"] = int(ask["date"].timestamp())
                if ask["sold"] is not None:
                    ask["sold"] = int(ask["sold"].timestamp())
                if ask["failed"] is not None:
                    ask["failed"] = int(ask["failed"].timestamp())

            return jsonify(ask_list)

        except mysql.connector.Error as e:
            print(f"j Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/missionoverview', methods=['GET'])
def missionoverview():
    user = request.args.get('user', None)

    if not user:
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Query to fetch userdata from 'users' table
            query_user = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query_user, (user,))
            userdata = cursor.fetchone()

            if not userdata:
                return jsonify([])

            missioncontrol = userdata.get("r_missioncontrol")
            missioncontrol_buff = userdata.get("b_missioncontrol")

            if missioncontrol is not None:
                max_missions = int(missioncontrol) * 2
            else:
                max_missions = 0

            if missioncontrol_buff is not None and missioncontrol_buff > datetime.utcnow():
                max_missions = 400

            # Query to count active missions for the user
            query_active_missions = """
                SELECT COUNT(*) AS active_missions
                FROM missions
                WHERE user = %s
                    AND (
                        (busy_until > CURRENT_TIMESTAMP AND busy_until_return IS NULL AND cancel_trx IS NULL)
                        OR (busy_until_return > CURRENT_TIMESTAMP AND cancel_trx IS NULL)
                        OR (mission_type = 'support' AND cancel_trx IS NULL)
                    )
            """
            cursor.execute(query_active_missions, (user,))
            active_missions_data = cursor.fetchone()
            active_missions = active_missions_data['active_missions'] if active_missions_data else 0
            free_missions = max_missions - active_missions

            # Query to count friendly and hostile missions
            query_friendly_hostile_missions = """
                SELECT mission_type
                FROM missions
                WHERE CONCAT(cords_hor_dest, '/', cords_ver_dest) IN (
                        SELECT CONCAT(cords_hor, '/', cords_ver)
                        FROM planets
                        WHERE user = %s
                    )
                    AND user != %s
                    AND (
                        (busy_until > CURRENT_TIMESTAMP AND busy_until_return IS NULL)
                        OR (busy_until_return > CURRENT_TIMESTAMP)
                        OR (mission_type IN ('support', 'breaksiege', 'deploy', 'transport'))
                    )
            """
            cursor.execute(query_friendly_hostile_missions, (user, user))
            mission_types = cursor.fetchall()
            friendly_count = sum(1 for mission in mission_types if mission['mission_type'] in ['support', 'breaksiege', 'deploy', 'transport'])
            hostile_count = sum(1 for mission in mission_types if mission['mission_type'] in ['siege', 'attack'])

            response = {
                "free_missions": free_missions,
                "max_missions": max_missions,
                "own_missions": active_missions,
                "hostile_missions": hostile_count,
                "friendly_missions": friendly_count
            }

            return jsonify(response)

        except mysql.connector.Error as e:
            print(f"k Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route("/dailybattles", methods=['GET'])
def dailybattles():
    try:
        connection = get_db_connection()

        if connection:
            cursor = connection.cursor(dictionary=True)

            # Construct SQL query
            sql = """SELECT mission_id, date, attacker, defender, (coal+2*ore+4*copper+8*uranium) AS points, result
                     FROM battleresults
                     WHERE date > %s
                     AND (coal+2*ore+4*copper+8*uranium) = (
                         SELECT MAX(coal+2*ore+4*copper+8*uranium) AS points
                         FROM battleresults
                         WHERE date > %s
                     )
                     ORDER BY date DESC
                     LIMIT 1"""
            
            # Calculate timestamp 24 hours ago
            timestamp_24h_ago = datetime.now(timezone.utc) - timedelta(hours=24)

            # Execute SQL query
            cursor.execute(sql, (timestamp_24h_ago, timestamp_24h_ago))
            result = cursor.fetchone()

            if result:
                loot_mission = result["mission_id"]
                loot_attacker = result["attacker"]
                loot_defender = result["defender"]
                loot_points = float(result["points"])
                loot_result = result["result"]
                loot_date = int(result["date"].timestamp())

                response = {
                    "loot_mission": loot_mission,
                    "loot_attacker": loot_attacker,
                    "loot_defender": loot_defender,
                    "loot_date": loot_date,
                    "loot_points": loot_points,
                    "loot_result": loot_result
                }
            else:
                response = {}

            return jsonify(response)

        else:
            return jsonify({})

    except mysql.connector.Error as e:
        print(f"l Error executing MySQL query: {e}")
        return jsonify({})

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

@app.route("/stardusttransfers", methods=['GET'])
def stardusttransfers():
    try:
        after_id = request.args.get('after_id', None)
        user = request.args.get('user', None)

        if user is None or after_id is None:
            return jsonify([])

        connection = get_db_connection()

        if connection:
            cursor = connection.cursor(dictionary=True)

            # Execute SQL query
            sql = f"SELECT * FROM stardust WHERE id > {after_id} AND (from_user = '{user}' OR to_user = '{user}') AND tr_type='transfer' ORDER BY id DESC"
            cursor.execute(sql)
            result = cursor.fetchall()

            transaction_list = []
            for transaction in result:
                transaction["date"] = int(transaction["date"].timestamp())
                transaction_list.append(transaction)

            return jsonify(transaction_list)

        else:
            return jsonify([])

    except mysql.connector.Error as e:
        print(f"m Error executing MySQL query: {e}")
        return jsonify([])

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


@app.route('/planetshipyard', methods=['GET'])
def planetshipyard():
    planet = request.args.get('planet', None)
    user = request.args.get('user', None)
    name = request.args.get('name', None)

    if not user or not planet:
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Fetch userdata from 'users' table
            query_user = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query_user, (user,))
            userdata = cursor.fetchone()

            if not userdata:
                return jsonify([])

            battlespeed_buff = userdata.get("b_battlespeed")
            apply_battlespeed = False

            if battlespeed_buff is not None:
                if battlespeed_buff > datetime.utcnow():
                    apply_battlespeed = True

            if name is not None:
                name_condition = "full_table.name = %s"
            else:
                name_condition = "1=1"

            # Query to fetch shipyard data
            query_shipyard = """
                SELECT *
                FROM (
                    SELECT *
                    FROM shipstats shipstats
                    INNER JOIN upgradecosts ON shipstats.name = upgradecosts.upgrade_type
                    LEFT JOIN (
                        SELECT type, MAX(busy_until) AS busy_until
                        FROM ships
                        WHERE user = %s
                          AND cords_hor IN (SELECT cords_hor FROM planets WHERE id = %s)
                          AND cords_ver IN (SELECT cords_ver FROM planets WHERE id = %s)
                        GROUP BY type
                    ) AS ships ON shipstats.name = ships.type
                    JOIN planets ON planets.id = %s
                    JOIN users ON users.username = %s
                ) AS full_table
                WHERE """ + name_condition
              
            cursor.execute(query_shipyard, (user, planet, planet, planet, user))
            result = cursor.fetchall()

            stack = []

            for row in result:
                activated = row['blueprints'] == row["name"]

                if row["busy_until"] is not None:
                    row["busy_until"] = int(row["busy_until"].timestamp())

                row["upgrade_time"] = row["upgrade_time"] * (1 - 0.01 * row["level_shipyard"])

                basespeed = row["speed"]

                if apply_battlespeed:
                    row["speed"] = row["battlespeed"]

                ship_skill = row.get('r_' + row["class"], None)

                stack.append({
                    "activated": activated,
                    "type": row["name"],
                    "speed": float(row["speed"]),
                    "consumption": float(row["consumption"]),
                    "longname": row["longname"],
                    "capacity": int(row["capacity"]),
                    "class": row["class"],
                    "variant": row["variant_name"],
                    "structure": int(row["structure"]),
                    "armor": int(row["armor"]),
                    "rocket": int(row["rocket"]),
                    "bullet": int(row["bullet"]),
                    "laser": int(row["laser"]),
                    "busy_until": row["busy_until"],
                    "blueprint": row["blueprint"],
                    "costs": {
                        "coal": row["coal"],
                        "ore": row["ore"],
                        "copper": row["copper"],
                        "uranium": row["uranium"],
                        "stardust": row["stardust"],
                        "time": row["upgrade_time"]
                    },
                    "shipyard_level": row["level_shipyard"],
                    "shipyard_skill": row["r_shipyard"],
                    "shipyard_min_level": row["shipyard_level"],
                    "ship_skill": ship_skill,
                    "variant_name": row["variant_name"],
                    "shield": row["shield"],
                    "basespeed": basespeed,
                    "battlespeed": row["battlespeed"],
                    "order": row["order"]
                })

            return jsonify(stack)

        except mysql.connector.Error as e:
            print(f"n Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/missioninfo', methods=['GET'])
def missioninfo():
    planet = request.args.get('planet', None)
    user = request.args.get('user', None)

    if user is None or not planet:
        return jsonify({})

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Fetch userdata from 'users' table
            query_user = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query_user, (user,))
            userdata = cursor.fetchone()

            if not userdata:
                return jsonify({})

            missioncontrol = 0
            missioncontrol_buff = userdata.get("b_missioncontrol")
            if userdata["r_missioncontrol"] is not None:
                missioncontrol = userdata["r_missioncontrol"]

            # Fetch planetdata from 'planets' table
            query_planet = "SELECT * FROM planets WHERE id = %s"
            cursor.execute(query_planet, (planet,))
            planetdata = cursor.fetchone()

            if not planetdata:
                return jsonify({})

            cords_hor = planetdata["cords_hor"]
            cords_ver = planetdata["cords_ver"]
            level_base = planetdata["level_base"]

            # Count running missions for the user
            query_running_missions = """
                SELECT COUNT(*) AS running_missions
                FROM missions
                WHERE user = %s
                    AND (
                        (busy_until > CURRENT_TIMESTAMP AND busy_until_return IS NULL)
                        OR (busy_until_return > CURRENT_TIMESTAMP)
                    )
            """
            cursor.execute(query_running_missions, (user,))
            running_missions_data = cursor.fetchone()
            running_missions = running_missions_data['running_missions'] if running_missions_data else 0

            # Count running missions on the planet
            query_running_planet_missions = """
                SELECT COUNT(*) AS running_planet_missions
                FROM missions
                WHERE user = %s
                    AND cords_hor = %s
                    AND cords_ver = %s
                    AND (
                        (busy_until > CURRENT_TIMESTAMP AND busy_until_return IS NULL)
                        OR (busy_until_return > CURRENT_TIMESTAMP)
                    )
            """
            cursor.execute(query_running_planet_missions, (user, cords_hor, cords_ver))
            running_planet_missions_data = cursor.fetchone()
            running_planet_missions = running_planet_missions_data['running_planet_missions'] if running_planet_missions_data else 0

            # Calculate allowed missions based on user and planet controls
            allowed_missions = missioncontrol * 2
            allowed_planet_missions = math.floor(level_base / 2)

            # Check if mission is allowed
            user_unused = allowed_missions - running_missions
            planet_unused = allowed_planet_missions - running_planet_missions
            mission_allowed = planet_unused > 0 and user_unused > 0

            response = {
                "user_max": allowed_missions,
                "user_active": running_missions,
                "planet_max": allowed_planet_missions,
                "planet_active": running_planet_missions,
                "user_unused": user_unused,
                "planet_unused": planet_unused,
                "mission_allowed": mission_allowed
            }

            return jsonify(response)

        except mysql.connector.Error as e:
            print(f"o Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/buffs', methods=['GET'])
def buffs():
    user = request.args.get('user', None)
    if user is None:
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Fetch userdata from 'users' table
            query_user = "SELECT * FROM users WHERE username = %s"
            cursor.execute(query_user, (user,))
            userdata = cursor.fetchone()

            if not userdata:
                return jsonify([])

            buffs = []
            # Fetch buffs from 'buffs' table
            query_buffs = "SELECT * FROM buffs"
            cursor.execute(query_buffs)
            buffs_data = cursor.fetchall()

            for row in buffs_data:
                buff_end = userdata.get("b_" + row["name"], None)
                buff_end = int(buff_end.timestamp()) if buff_end else 0

                buffs.append({
                    "name": row["name"],
                    "price": row["price"],
                    "buff_end": buff_end,
                    "buff_duration": row["buff_duration"]
                })

            return jsonify(buffs)

        except mysql.connector.Error as e:
            print(f"p Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()

    else:
        return jsonify({"error": "Database connection error"}), 500

@app.route('/yamatotracker', methods=['GET'])
def yamatotracker():
    yamato_list = []
    now = datetime.utcnow()
    busy = request.args.get('busy', '2')  # Default value for busy is '2'
    
    if busy not in ['0', '1', '2']:
        return jsonify([])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            # Query based on busy status
            if busy == '1':
                query = "SELECT * FROM ships WHERE type LIKE '%yamato%' AND mission_busy_until > CURRENT_TIMESTAMP ORDER BY mission_busy_until DESC"
            elif busy == '0':
                query = "SELECT * FROM ships WHERE type LIKE '%yamato%' AND mission_busy_until < CURRENT_TIMESTAMP ORDER BY mission_busy_until DESC"
            else:
                query = "SELECT * FROM ships WHERE type LIKE '%yamato%' ORDER BY mission_busy_until DESC"

            cursor.execute(query)
            yamatodata = cursor.fetchall()

            if not yamatodata:
                return jsonify([])

            for ship in yamatodata:
                ship_type = ship['type']
                cords_hor = ship['cords_hor']
                cords_ver = ship['cords_ver']
                busy_until = ship['mission_busy_until']
                upgrade = 1 if busy_until > now else 0
                owner = ship['user']
                busy_until = int(busy_until.timestamp())
                yamato_list.append({
                    "type": ship_type,
                    "cords_hor": cords_hor,
                    "cords_ver": cords_ver,
                    "owner": owner,
                    "upgrade": upgrade,
                    "upgrade_until": busy_until
                })

            return jsonify(yamato_list)

        except mysql.connector.Error as e:
            print(f"q Error executing MySQL query: {e}")
            return jsonify({"error": "Database error"}), 500

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()
    else:
        return jsonify({"error": "Database connection error"}), 500


@app.route('/marketstats', methods=['GET'])
def marketstats():
    try:
        marketdata = {}
        connection = get_db_connection()

        if connection:
            cursor = connection.cursor(dictionary=True)

            # Count ships on market
            cursor.execute('SELECT COUNT(*) AS ship_count FROM asks WHERE category = "ship" AND sold IS NULL AND cancel_trx IS NULL')
            row = cursor.fetchone()
            ships_on_market = int(row['ship_count']) if row else 0

            # Count planets on market
            cursor.execute('SELECT COUNT(*) AS planet_count FROM asks WHERE category = "planet" AND sold IS NULL AND cancel_trx IS NULL')
            row = cursor.fetchone()
            planets_on_market = int(row['planet_count']) if row else 0

            # Count items on market
            cursor.execute('SELECT COUNT(*) AS item_count FROM asks WHERE category = "item" AND sold IS NULL AND cancel_trx IS NULL')
            row = cursor.fetchone()
            items_on_market = int(row['item_count']) if row else 0

            # Count transactions
            cursor.execute('SELECT COUNT(*) AS transaction_count FROM asks WHERE sold IS NOT NULL')
            row = cursor.fetchone()
            transaction_number = int(row['transaction_count']) if row else 0

            # Calculate trading volume
            cursor.execute('SELECT SUM(price) AS total_volume FROM asks WHERE sold IS NOT NULL')
            row = cursor.fetchone()
            trading_volume = int(row['total_volume']) if row and row['total_volume'] else 0

            # Calculate total fee burned
            cursor.execute('SELECT SUM(fee_burn) AS total_fee_burned FROM asks WHERE sold IS NOT NULL')
            row = cursor.fetchone()
            total_fee_burned = int(row['total_fee_burned']) if row and row['total_fee_burned'] else 0

            # Calculate total stardust burned
            cursor.execute('SELECT SUM(amount) AS total_sent_null FROM stardust WHERE tr_type = "market" AND to_user = "null"')
            row = cursor.fetchone()
            total_sent_null = int(row['total_sent_null']) if row and row['total_sent_null'] else 0
            total_burned = total_sent_null + total_fee_burned

            # Find highest sale
            cursor.execute('SELECT * FROM asks WHERE sold IS NOT NULL ORDER BY price DESC LIMIT 1')
            row = cursor.fetchone()
            if row:
                highest_sale = {
                    "type": row['type'],
                    "category": row['category'],
                    "seller": row['user'],
                    "price": int(row['price'])
                }
            else:
                highest_sale = {"price" : 0}

            marketdata = {
                "planets_on_market": planets_on_market,
                "ships_on_market": ships_on_market,
                "items_on_market": items_on_market,
                "transaction_number": transaction_number,
                "trading_volume": trading_volume,
                "total_burned": total_burned,
                "highest_sale": highest_sale
            }

            return jsonify(marketdata)

        else:
            return jsonify([])

    except mysql.connector.Error as e:
        print(f"r Error executing MySQL query: {e}")
        return jsonify([])

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

if __name__ == '__main__':
    app.run(port=5000, debug=True)
