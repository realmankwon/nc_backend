import json
from datetime import timedelta, datetime
from beem.block import Block
from beem.amount import Amount
import requests
import time
import requests
import hashlib
import ast
import string

import random
import websocket
import sys
from random import randint
from utils.ncutils import checkifuser, findfreeplanet, shipdata, connectdb, get_shipdata,get_planetdata,get_distance,create_planet
from utils.ncutils import find_starterplanet_coords, generateUid, uid_from_block, write_spacedb, get_ship_data, get_item_data, get_planet_data, update_transaction_status, update_transfer_status
from utils.ncutils import get_custom_json_id, get_transfer_id, get_mission_data, get_ask_data
from commands import move_ship, explorespace, transport_resources, offload_deploy, offload_return, get_resource_levels, build_ship, enhance
from commands import upgrade, activate, adduser, buy, explore, finish_building, finish_skill, gift_item, update_ranking, gift_planet, deploy_ships, rename_planet
from commands import update_shop, attack, battle_return, cancel, support, enable, charge, finish_charging, offload_deploy_mission, siege
from commands import break_siege, fly_home_mission, offload_return_mission, issue, transfer_stardust, new_season, upgrade_yamato, finish_yamato, finish_season
from commands import respawn, burn, issuestardust, ask, cancel_ask, fill_ask, buff, updatebuff
from db_connection import get_db_connection
import mysql.connector
from mysql.connector import Error

# get the productivity data from the SQL DB
# Connect to the database

def get_parameter():
    parameter = {}

    # 연결 생성
    connection = get_db_connection()
    if connection is None:
        print("Database connection failed.")
        return
    
    try:
        # 업그레이드 비용 데이터 수집
        upgrade_keys = ["shipyard", "oredepot", "copperdepot", "coaldepot", "uraniumdepot", "explorership",
                        "transportship", "scout", "patrol", "cutter", "corvette", "frigate", "destroyer", "cruiser", "battlecruiser",
                        "carrier", "dreadnought", "yamato", "yamato1", "yamato2", "yamato3", "yamato4", "yamato5", "yamato6", "yamato7", "yamato8", "yamato9", "yamato10", "yamato11", "yamato12",
                        "yamato13", "yamato14", "yamato15", "yamato16", "yamato17", "yamato18", "yamato19", "yamato20", "oremine", "coppermine", "coalmine", "uraniummine", "base",
                        "researchcenter", "bunker", "shieldgenerator", "explorership1", "transportship1", "transportship2"]
        upgrade_costs = {}
        cursor = connection.cursor(dictionary=True)

        query = "SELECT * FROM upgradecosts"
        cursor.execute(query)
        upgradecosts = cursor.fetchall()

        for upgradecost in upgradecosts:
            name = upgradecost["name"]
            if name in upgrade_costs:
                # 딕셔너리에 키가 이미 존재하는 경우
                if str(len(upgrade_costs[name]) + 1) not in upgrade_costs[name]:
                    upgrade_costs[name][str(len(upgrade_costs[name]) + 1)] = upgradecost
                # 여기서 value를 사용하여 작업을 진행할 수 있음
            else:
                # 딕셔너리에 키가 존재하지 않는 경우 처리
                upgrade_costs[name] = {str(1): upgradecost}

        # for key in upgrade_keys:
        #     upgrade_costs[key] = {}
        #     for x in range(1, 21):
        #         result = table.find_one(name=key, level=x)
        #         if result is not None:
        #             upgrade_costs[key][str(x)] = result
        parameter["upgrade_costs"] = upgrade_costs

        # 스킬 비용 데이터 수집
        skill_keys = ["shipyard", "oredepot", "copperdepot", "coaldepot", "uraniumdepot", "Explorer",
                      "Transporter", "Scout", "Patrol", "Cutter", "Corvette", "Frigate", "Destroyer", "Cruiser", "Battlecruiser",
                      "Carrier", "Dreadnought", "Yamato", "oremine", "coppermine", "coalmine", "uraniummine", "base", "researchcenter",
                      "orebooster", "coalbooster", "copperbooster", "uraniumbooster", "missioncontrol", "bunker",
                      "enlargebunker", "structureimprove", "armorimprove", "shieldimprove",
                      "rocketimprove", "bulletimprove", "laserimprove", "regenerationbonus", "repairbonus",
                      "shieldgenerator", "siegeprolongation", "depotincrease"]
        skill_costs = {}
        
        query = "SELECT * FROM skillcosts"
        cursor.execute(query)
        skillcosts = cursor.fetchall()

        for skillcost in skillcosts:
            name = skillcost["name"]
            if name in skill_costs:
                # 딕셔너리에 키가 이미 존재하는 경우
                if str(len(skill_costs[name]) + 1) not in skill_costs[name]:
                    skill_costs[name][str(len(skill_costs[name]) + 1)] = skillcost
                # 여기서 value를 사용하여 작업을 진행할 수 있음
            else:
                # 딕셔너리에 키가 존재하지 않는 경우 처리
                skill_costs[name] = {str(1): skillcost}

        parameter["skill_costs"] = skill_costs

        # 생산률 데이터 수집
        production_keys = ["coalmine", "oremine", "coppermine", "uraniummine", "coaldepot", "oredepot", "copperdepot", "uraniumdepot"]
        production_rates = {}
        
        query = "SELECT * FROM productivity"
        cursor.execute(query)
        productivity = cursor.fetchall()

        for product in productivity:
            name = product["name"]
            if name in production_rates:
                # 딕셔너리에 키가 이미 존재하는 경우
                if str(len(production_rates[name]) + 1) not in production_rates[name]:
                    production_rates[name][str(len(production_rates[name]))] = product
                # 여기서 value를 사용하여 작업을 진행할 수 있음
            else:
                # 딕셔너리에 키가 존재하지 않는 경우 처리
                production_rates[name] = {str(0): product}

        parameter["production_rates"] = production_rates

        # 행성 희귀도 데이터 수집
        planet_rarity = {}
        
        query = "SELECT * FROM planetlevels"
        cursor.execute(query)
        rarity = cursor.fetchall()

        for data in rarity:
            planet_rarity[data["rarity"]] = data
        parameter["planet_rarity"] = planet_rarity

        # 함선 통계 데이터 수집
        shipstats = {}
        
        query = "SELECT * FROM shipstats"
        cursor.execute(query)
        ships = cursor.fetchall()

        for result in ships:
            if result is not None:
                shipstats[result["name"]] = result
        parameter["shipstats"] = shipstats

        return parameter
    
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    
    except Exception as err:
        print(f"Error: {err}")
    
    finally:
        # 연결 닫기
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def get_transaction():
    # get all open transactions from the database
    # Connect to the database
        
    connection = get_db_connection()
    if connection is None:
        print("Database connection failed.")
        return
    
    cursor = connection.cursor(dictionary=True)

    query = "SELECT * FROM transactions where tr_status = 0 ORDER BY id LIMIT 1"
    cursor.execute(query)
    trx = cursor.fetchone()

    parameter = get_parameter()

    start_time = time.time()

    id = trx['id']
    user = trx['user']
    trx_id = trx['trx']
    block_num = trx['block_num']
    tr_type = trx['tr_type']
    tr_var1 = trx['tr_var1']
    tr_var2 = trx['tr_var2']
    tr_var3 = trx['tr_var3']
    tr_var4 = trx['tr_var4']
    tr_var5 = trx['tr_var5']
    tr_var6 = trx['tr_var6']
    tr_var7 = trx['tr_var7']
    tr_var8 = trx['tr_var8']
    transaction_valid = True
    time_now = trx['createdAt'].replace(tzinfo=None)
    
    success = False
    if (datetime.utcnow() - time_now).total_seconds() < 12: 
        update_ranking(parameter, time_now)
    check_ships = False
      
    if check_ships:
        connection = get_db_connection()
        if connection is None:
            print("Failed to connect to database")
            exit()

        cursor = connection.cursor(dictionary=True)

        # ships 테이블에서 모든 ship의 id 가져오기
        query = "SELECT id FROM ships"
        cursor.execute(query)
        ships = cursor.fetchall()

        print(f"{len(ships)} ships found")

        cnt = 0
        for ship in ships:
            cnt += 1
            shipid = ship["id"]
            if cnt % 1000 == 0:
                print(f"{cnt}/{len(ships)} ships processed")

            # ships 테이블에서 shipid에 해당하는 ship 정보 가져오기
            query = "SELECT * FROM ships WHERE id = %s"
            cursor.execute(query, (shipid,))
            ship = cursor.fetchone()

            if ship is None:
                continue

            mission_id = ship["mission_id"]

            # missions 테이블에서 mission_id에 해당하는 mission 정보 가져오기
            query = "SELECT * FROM missions WHERE mission_id = %s"
            cursor.execute(query, (mission_id,))
            mission = cursor.fetchone()

            if mission is None and datetime(2019, 6, 9, 18, 32, 0) < time_now < datetime(2019, 7, 1, 8, 22, 0):
                # transactions 테이블에서 shipid와 관련된 transaction 정보 가져오기
                query = "SELECT * FROM transactions WHERE tr_var1 = %s ORDER BY date DESC LIMIT 1"
                cursor.execute(query, (shipid,))
                transaction = cursor.fetchone()

                if transaction is None:
                    continue

                # missions 테이블에서 transaction["tr_var3"] 또는 transaction["tr_var4"]에 해당하는 mission 정보 가져오기
                query = "SELECT * FROM missions WHERE mission_id = %s OR mission_id = %s"
                cursor.execute(query, (transaction["tr_var3"], transaction["tr_var4"]))
                mission = cursor.fetchone()

            if mission is None:
                continue

            if ship["user"] != mission["user"]:
                continue

            if abs((time_now - datetime(2019, 7, 1, 8, 22, 30)).total_seconds()) < 5:
                if ship["cords_hor"] != mission["cords_hor_dest"] and not (ship["cords_hor"] == mission["cords_hor"] and ship["cords_ver"] == mission["cords_hor"]):
                    continue
                if ship["cords_ver"] != mission["cords_ver_dest"] and not (ship["cords_hor"] == mission["cords_hor"] and ship["cords_ver"] == mission["cords_hor"]):
                    continue
            else:
                if ship["cords_hor"] != mission["cords_hor_dest"]:
                    continue
                if ship["cords_ver"] != mission["cords_ver_dest"]:
                    continue

            if mission["busy_until_return"] is None:
                continue

            if mission["busy_until_return"] > time_now:
                continue

            vops_type = None
            if mission["mission_type"] == "siege":
                # virtualops 테이블에서 mission_id에 해당하는 vops 정보 가져오기
                query = "SELECT * FROM virtualops WHERE mission_id = %s"
                cursor.execute(query, (mission_id,))
                vops = cursor.fetchone()

                if vops is not None:
                    vops_type = vops["tr_type"]

            if ship["qyt_uranium"] == 0 and vops_type != "offload_deploy_mission":
                continue

            if vops_type != "offload_deploy_mission":
                # planets 테이블에서 mission의 cords_hor, cords_ver에 해당하는 planet 정보 가져오기
                query = "SELECT * FROM planets WHERE cords_hor = %s AND cords_ver = %s"
                cursor.execute(query, (mission["cords_hor"], mission["cords_ver"]))
                planet = cursor.fetchone()

                if planet is None:
                    continue

                to_planet_id = planet["id"]
                offload_return(shipid, to_planet_id, mission_id, parameter, time_now, block_num, trx_id)
            else:
                # ships 테이블 업데이트
                query = "UPDATE ships SET cords_hor = %s, cords_ver = %s WHERE id = %s"
                cursor.execute(query, (int(mission["cords_hor"]), int(mission["cords_ver"]), shipid))
                connection.commit()               
    
    if tr_type == "transport":
        usr = get_planet_data(tr_var2,"user")
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))        
        if usr == user:
            success = transport_resources(tr_var1, tr_var2, tr_var3,tr_var4,tr_var5,tr_var6, tr_var7,tr_var8,  parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False
  
    elif tr_type == "offload_deploy_mission":
        usr = get_mission_data(tr_var1,"user")
        if usr == user and virtualop:
            success = offload_deploy_mission(tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "offload_return_mission":
        usr = get_mission_data(tr_var1,"user")
        if usr == user and virtualop:
            success = offload_return_mission(tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False   

    elif tr_type == "fly_home_mission":
        usr = get_mission_data(tr_var1,"user")
        if usr == user and virtualop:
            success = fly_home_mission(tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:

            transaction_valid = False                
    elif tr_type == "offload_deploy":
        usr = get_ship_data(tr_var1,"user")
        if usr == user and virtualop:
            success = offload_deploy(tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "offload_return":
        usr = get_ship_data(tr_var1,"user")
        if usr == user and virtualop:
            success = offload_return(tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "deploy":
        print(type(tr_var1))
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))
        if tr_var8 is None and isinstance(tr_var1, str):
            if tr_var[0] == '[':
                tr_var = tr_var[1:-1]
            if tr_var1.find(",") > -1:
                ship_list = tr_var1.split(",")
            elif tr_var1.find(";") > -1:
                ship_list = tr_var1.split(";")        
            else:
                ship_list = [tr_var1]            
            usr = get_ship_data(ship_list[0],"user")
        elif tr_var8 is None and isinstance(tr_var1, list) and len(tr_var1) > 0:
            usr = get_ship_data(tr_var1[0],"user")
        else:
            print(tr_var8)
            usr = get_planet_data(tr_var8,"user")
        print(usr)
        if usr == user:
            success = deploy_ships(tr_var1, tr_var2, tr_var3,tr_var4,tr_var5,tr_var6, tr_var7, tr_var8, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "attack":
        print(type(tr_var1))
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))

        usr = get_planet_data(tr_var4,"user")
        print(usr)
        if usr == user:
            success = attack(tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "breaksiege":
        print(type(tr_var1))
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))

        usr = get_planet_data(tr_var4,"user")
        print(usr)
        if usr == user:
            success = break_siege(tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "support":
        print(type(tr_var1))
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))

        usr = get_planet_data(tr_var4,"user")
        print(usr)
        if usr == user:
            success = support(tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "siege":
        print(type(tr_var1))
        try:
            tr_var1 = json.loads(tr_var1.replace("'", '"'))
        except:
            print(type(tr_var1))

        usr = get_planet_data(tr_var4,"user")
        print(usr)
        if usr == user:
            success = siege(tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False
            
    elif tr_type == "battle_return":
        usr = get_mission_data(tr_var1,"user")
        if usr == user and virtualop:
            success = battle_return(tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "cancel":
        usr = get_mission_data(tr_var1,"user")
        if usr == user:
            success = cancel(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "enhance":
        usr = tr_var1
        if usr == user:
            success = enhance(tr_var1, tr_var2, tr_var3, parameter, time_now, trx_id, id)   
            update_transaction_status(success, id)      
        else:
            transaction_valid = False

    elif tr_type == "newuser":
        usr = tr_var1
        if usr == user:
            success = adduser(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "respawn":
        usr = get_planet_data(tr_var1,"user")       
        if usr == user:   
            success = respawn(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "burn":
        usr = get_planet_data(tr_var1,"user")     
        if usr == user:
            success = burn(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "buildship":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:       
            success = build_ship(tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "explorespace":
        usr = get_planet_data(tr_var1,"user")     
        if usr  == user:      
            success = explorespace(tr_var1,tr_var2,tr_var3, tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False
            
    elif tr_type == "explore":
        usr_ship = get_ship_data(tr_var1,"user") 
        if usr_ship  == user and virtualop: 
            success = explore(tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            print(usr_ship)
            print("explore not valid")
            transaction_valid = False
            
    elif tr_type == "finishbuilding":
        if virtualop:       
            success = finish_building(tr_var1,tr_var2,tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "finishcharging":
        if virtualop:        
            success = finish_charging(tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "finishskill":
        if virtualop:    
            success = finish_skill(tr_var1,tr_var2,tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "upgrade":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = upgrade(tr_var1,tr_var2, parameter, time_now, trx_id, id)
            update_transaction_status(success, id)            
        else:
            transaction_valid = False

    elif tr_type == "charge":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = charge(tr_var1,tr_var2, parameter, time_now, trx_id, id)
            update_transaction_status(success, id) 
        else:
            transaction_valid = False

    elif tr_type == "enable":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = enable(tr_var1,tr_var2, parameter, time_now, trx_id, id)    
            update_transaction_status(success, id)         
        else:
            transaction_valid = False

    elif tr_type == "activate":
        usr = get_item_data(tr_var1, "owner")
        if usr == user:
            success = activate(tr_var1, tr_var2, parameter, time_now, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "giftitem":
        usr = get_item_data(tr_var1, "owner")
        if usr == user:
            success = gift_item(tr_var1, tr_var2, parameter, time_now, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "transferstardust":
        success = transfer_stardust(user, tr_var1, tr_var2, parameter, time_now, trx_id)
        update_transaction_status(success, id)

    elif tr_type == "giftplanet":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = gift_planet(tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "renameplanet":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = rename_planet(tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False 

    elif tr_type == "updateshop":
        if user in ["nextcolony"]:
            success = update_shop(tr_var1, tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "newseason":
        if user in ["nextcolony"]:
            success = new_season(tr_var1, tr_var2, tr_var3, tr_var4, tr_var5, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False  

    elif tr_type == "finishseason":
        if virtualop:
            success = finish_season(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "issue":
        if user in ["nextcolony"]:
            success = issue(tr_var1, tr_var2, tr_var3, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False    

    elif tr_type == "issuestardust":
        if user in ["nextcolony"]:
            success = issuestardust(tr_var1, tr_var2, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False 

    elif tr_type == "upgradeyamato":
        usr = get_planet_data(tr_var1,"user")
        if usr == user:
            success = upgrade_yamato(usr, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False 

    elif tr_type == "finishyamato":
        if virtualop:
            success = finish_yamato(tr_var1,tr_var2,tr_var3, tr_var4, tr_var5, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "ask":
        if tr_var1 == "ship":          
            usr = get_ship_data(tr_var2,"user")
        elif tr_var1 == "item":
            usr = get_item_data(tr_var2,"owner")
        elif tr_var1 == "planet":
            usr = get_planet_data(tr_var2,"user")
        else:
            usr = None
        if usr is not None and usr == user:
            success = ask(usr, tr_var1, tr_var2, tr_var3, tr_var4, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False   

    elif tr_type == "cancel_ask":
        usr = get_ask_data(tr_var1,"user")
        if usr == user:
            success = cancel_ask(tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False

    elif tr_type == "fill_ask":
        success = fill_ask(user, tr_var1, parameter, time_now, block_num, trx_id)
        update_transaction_status(success, id)

    elif tr_type == "buff":
        success = buff(user, tr_var1, parameter, time_now, block_num, trx_id)
        update_transaction_status(success, id)

    elif tr_type == "updatebuff":
        if user in ["nextcolony"]:
            success = updatebuff(tr_var1, tr_var2, time_now, block_num, trx_id)
            update_transaction_status(success, id)
        else:
            transaction_valid = False 

    else:
        transaction_valid = False

    delay_min = (datetime.utcnow() - time_now).total_seconds() / 60
    delay_sec = int((datetime.utcnow() - time_now).total_seconds())
    duration_sec = (time.time() - start_time)
    if delay_min < 1:
        print("%s (+ %d s): %s wants %s (%s, %s, %s)-> sucess: %s (dur. %.2f s)" % (str(time_now), delay_sec, user, tr_type, tr_var1, tr_var2, tr_var3, str(success), duration_sec))
    else:
        print("%s (+ %.1f min): %s wants %s (%s, %s, %s)-> sucess: %s (dur. %.2f s)" % (str(time_now), delay_min, user, tr_type, tr_var1, tr_var2, tr_var3, str(success), duration_sec))

    if not transaction_valid:
        update_transaction_status(False, id)

def get_transfer(trx):

    id = trx['id']
    user = trx['user']
    memo = trx['memo']
    trx_id = trx['trx']
    block_num = trx['block_num']
    time_now = trx['date']
    amount = Amount(trx["amount"])
    transfer_id = get_transfer_id()
    at_symbol_pos = memo.find('@')
    if at_symbol_pos < 0:
        print("memo does not start with %s@: %s" % (transfer_id, memo))
        update_transfer_status(False, id)
        return False    
    if len(memo) < at_symbol_pos + 1 or memo[:at_symbol_pos] != transfer_id:
        print("memo does not start with %s@: %s" % (transfer_id, memo[:at_symbol_pos + 1]))
        update_transfer_status(False, id)
        return False
    try:
        data = json.loads(memo[at_symbol_pos + 1:])
    except:
        try:
            data = ast.literal_eval(memo[at_symbol_pos + 1::])
        except:
            print("memo is not a json %s" % memo[at_symbol_pos + 1::])
            update_transfer_status(False, id)
            return False         
    transfer_valid = True
    if "command" not in data:
        print("command not in data: %s" % str(data))
        update_transfer_status(False, id)
        return False            
    if data["type"] == "auctionbid":
        update_transfer_status(True, id)
        return True
    elif data["type"] == "buy":
        command = data["command"]
        if command["user"] == "":
            command["user"] = user
        success = buy(command, amount, time_now, block_num, trx_id, id)
        print("%s: %s - %s -> sucess: %s" % (str(time_now), user, data["type"], str(success)))

get_transaction()