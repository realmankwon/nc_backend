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
import base36
import random
import websocket
import sys

from random import randint
from utils.ncutils import checkifuser, findfreeplanet, shipdata, connectdb, get_shipdata,get_planetdata,get_distance,create_planet
from utils.ncutils import find_starterplanet_coords, generateUid, uid_from_block, write_spacedb, get_ship_data, get_item_data, get_planet_data, update_transaction_status, update_transfer_status
from utils.ncutils import get_mission_data, get_ask_data, read_parameter
from commands import move_ship, explorespace, transport_resources, offload_deploy, offload_return, get_resource_levels, build_ship, enhance
from commands import upgrade, activate, adduser, buy, explore, finish_building, finish_skill, gift_item, update_ranking, gift_planet, deploy_ships, rename_planet
from commands import update_shop, attack, battle_return, cancel, support, enable, charge, finish_charging, offload_deploy_mission, siege
from commands import break_siege, fly_home_mission, offload_return_mission, issue, transfer_stardust, new_season, upgrade_yamato, finish_yamato, finish_season
from commands import respawn, burn, issuestardust, ask, cancel_ask, fill_ask, buff, updatebuff
from dotenv import load_dotenv
load_dotenv()
# get the productivity data from the SQL DB
# Connect to the database

def get_transaction(connection, trx, parameter):
    try:
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
        virtualop = trx['virtualop']
        transaction_valid = True
        time_now = trx['date'].replace(tzinfo=None)
        
        success = False
        if (datetime.now() - time_now).total_seconds() < 12: 
            update_ranking(connection, parameter, time_now)
        check_ships = False
        # if abs((time_now - datetime(2019, 6, 9, 6, 22, 30)).total_seconds()) < 10:
        #     check_ships = True    
        # elif abs((time_now - datetime(2019, 6, 9, 9, 22, 30)).total_seconds()) < 10:
        #     check_ships = True
        # elif abs((time_now - datetime(2019, 6, 9, 18, 35, 0)).total_seconds()) < 10:
        #     check_ships = True   
        # elif abs((time_now - datetime(2019, 6, 11, 9, 22, 30)).total_seconds()) < 10:
        #     check_ships = True
        # elif abs((time_now - datetime(2019, 6, 27, 16, 9, 0)).total_seconds()) < 10:
        #     check_ships = True
        # elif abs((time_now - datetime(2019, 7, 1, 8, 22, 30)).total_seconds()) < 5:
        #     check_ships = True
        # elif abs((time_now - datetime(2019, 7, 1, 12, 0, 0)).total_seconds()) < 10:
        #     check_ships = True        
        # elif abs((time_now - datetime(2019, 7, 1, 16, 30, 0)).total_seconds()) < 10:
        #     check_ships = True
        # elif abs((time_now - datetime(2019, 7, 13, 14, 40, 0)).total_seconds()) < 10:
        #     check_ships = True
        
    
        if check_ships:
            table = connection["ships"]
            ships = []
            for ship in table.find():
                ships.append(ship["id"])
            print("%d ships found" % len(ships))
            cnt = 0
            for shipid in ships:
                cnt += 1
                if cnt % 1000 == 0:
                    print("%d/%d ships processed" % (cnt, len(ships)))
                table = connection["ships"]
                ship = table.find_one(id=shipid)
                if ship is None:
                    continue
                if ship["mission_busy_until"] > time_now:
                    continue            
                mission_id = ship["mission_id"]
                table = connection["missions"]
                mission = table.find_one(mission_id=mission_id)
                if mission is None and time_now > datetime(2019, 6, 9, 18, 32, 0) and time_now < datetime(2019, 7, 1, 8, 22, 0):
                    
                    table = connection["transactions"]
                    transaction = table.find_one(tr_var1=shipid, order_by="-date")
                    if transaction is None:
                        continue
                    table = connection["missions"]
                    mission = table.find_one(mission_id=transaction["tr_var3"])
                    if mission is None:
                        mission = table.find_one(mission_id=transaction["tr_var4"])            
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
                    table = connection["virtualops"]
                    vops = table.find_one(mission_id=mission_id)
                    if vops is not None:
                        vops_type =vops["tr_type"]            
                if ship["qyt_uranium"] == 0 and vops_type != "offload_deploy_mission":
                    continue
        
                if vops_type != "offload_deploy_mission":
                    table = connection["planets"]
                    planet = table.find_one(cords_hor=mission["cords_hor"], cords_ver=mission["cords_ver"])
                    to_planet_id = planet["id"]                
                    offload_return (connection, shipid, to_planet_id, mission_id, parameter, time_now, block_num, trx_id)
                else:
                    table = connection['ships']
                    table.update({"id": str(shipid), "cords_hor": int(mission["cords_hor"]), "cords_ver": int(mission["cords_ver"])},['id'])                 
        
        if tr_type == "transport":
            usr = get_planet_data(tr_var2,"user")
            try:
                tr_var1 = json.loads(tr_var1.replace("'", '"'))
            except:
                print(type(tr_var1))        
            if usr == user:
                success = transport_resources(connection, tr_var1, tr_var2, tr_var3,tr_var4,tr_var5,tr_var6, tr_var7,tr_var8,  parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False
    
        elif tr_type == "offload_deploy_mission":
            usr = get_mission_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = offload_deploy_mission(connection, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "offload_return_mission":
            usr = get_mission_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = offload_return_mission(connection, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False   

        elif tr_type == "fly_home_mission":
            usr = get_mission_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = fly_home_mission(connection, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:

                transaction_valid = False                
        elif tr_type == "offload_deploy":
            usr = get_ship_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = offload_deploy(connection, tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "offload_return":
            usr = get_ship_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = offload_return(connection, tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
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
                usr = get_ship_data(connection, ship_list[0],"user")
            elif tr_var8 is None and isinstance(tr_var1, list) and len(tr_var1) > 0:
                usr = get_ship_data(connection, tr_var1[0],"user")
            else:
                print(tr_var8)
                usr = get_planet_data(connection, tr_var8,"user")
            print(usr)
            if usr == user:
                success = deploy_ships(connection, tr_var1, tr_var2, tr_var3,tr_var4,tr_var5,tr_var6, tr_var7, tr_var8, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "attack":
            print(type(tr_var1))
            try:
                tr_var1 = json.loads(tr_var1.replace("'", '"'))
            except:
                print(type(tr_var1))

            usr = get_planet_data(connection, tr_var4,"user")
            print(usr)
            if usr == user:
                success = attack(connection, tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "breaksiege":
            print(type(tr_var1))
            try:
                tr_var1 = json.loads(tr_var1.replace("'", '"'))
            except:
                print(type(tr_var1))

            usr = get_planet_data(connection, tr_var4,"user")
            print(usr)
            if usr == user:
                success = break_siege(connection, tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "support":
            print(type(tr_var1))
            try:
                tr_var1 = json.loads(tr_var1.replace("'", '"'))
            except:
                print(type(tr_var1))

            usr = get_planet_data(connection, tr_var4,"user")
            print(usr)
            if usr == user:
                success = support(connection, tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "siege":
            print(type(tr_var1))
            try:
                tr_var1 = json.loads(tr_var1.replace("'", '"'))
            except:
                print(type(tr_var1))

            usr = get_planet_data(connection, tr_var4,"user")
            print(usr)
            if usr == user:
                success = siege(connection, tr_var1, tr_var2, tr_var3,tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False
                
        elif tr_type == "battle_return":
            usr = get_mission_data(connection, tr_var1,"user")
            if usr == user and virtualop:
                success = battle_return(connection, tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
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
                success = enhance(connection, tr_var1, tr_var2, tr_var3, parameter, time_now, trx_id, id)   
                update_transaction_status(connection, success, id)      
            else:
                transaction_valid = False

        elif tr_type == "newuser":
            usr = tr_var1
            if usr == user:
                success = adduser(connection, tr_var1, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "respawn":
            usr = get_planet_data(connection, tr_var1,"user")       
            if usr == user:   
                success = respawn(connection, tr_var1, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "burn":
            usr = get_planet_data(connection, tr_var1,"user")     
            if usr == user:
                success = burn(connection, tr_var1, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "buildship":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:       
                success = build_ship(connection, tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "explorespace":
            usr = get_planet_data(connection, tr_var1,"user")     
            if usr  == user:      
                success = explorespace(connection, tr_var1,tr_var2,tr_var3, tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False
                
        elif tr_type == "explore":
            usr_ship = get_ship_data(connection, tr_var1,"user") 
            if usr_ship  == user and virtualop: 
                success = explore(connection, tr_var1,tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                print(usr_ship)
                print("explore not valid")
                transaction_valid = False
                
        elif tr_type == "finishbuilding":
            if virtualop:       
                success = finish_building(connection, tr_var1,tr_var2,tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "finishcharging":
            if virtualop:        
                success = finish_charging(connection, tr_var1,tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "finishskill":
            if virtualop:    
                success = finish_skill(connection, tr_var1,tr_var2,tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "upgrade":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = upgrade(connection, user, tr_var1,tr_var2, parameter, time_now, trx_id, id)
                update_transaction_status(connection, success, id)            
            else:
                transaction_valid = False

        elif tr_type == "charge":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = charge(connection, user, tr_var1,tr_var2, parameter, time_now, trx_id, id)
                update_transaction_status(connection, success, id) 
            else:
                transaction_valid = False

        elif tr_type == "enable":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = enable(connection, user, tr_var1,tr_var2, parameter, time_now, trx_id, id)    
                update_transaction_status(connection, success, id)         
            else:
                transaction_valid = False

        elif tr_type == "activate":
            usr = get_item_data(connection, tr_var1, "owner")
            if usr == user:
                success = activate(connection, tr_var1, tr_var2, parameter, time_now, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "giftitem":
            usr = get_item_data(connection, tr_var1, "owner")
            if usr == user:
                success = gift_item(connection, tr_var1, tr_var2, parameter, time_now, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "transferstardust":
            success = transfer_stardust(connection, user, tr_var1, tr_var2, parameter, time_now, trx_id)
            update_transaction_status(connection, success, id)

        elif tr_type == "giftplanet":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = gift_planet(connection, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "renameplanet":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = rename_planet(connection, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False 

        elif tr_type == "updateshop":
            if user in ["nextcolony"]:
                success = update_shop(connection, tr_var1, tr_var2, tr_var3, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "newseason":
            if user in ["nextcolony"]:
                success = new_season(connection, tr_var1, tr_var2, tr_var3, tr_var4, tr_var5, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False  

        elif tr_type == "finishseason":
            if virtualop:
                success = finish_season(connection, tr_var1, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "issue":
            if user in ["nextcolony"]:
                success = issue(connection, tr_var1, tr_var2, tr_var3, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False    

        elif tr_type == "issuestardust":
            if user in ["nextcolony"]:
                success = issuestardust(connection, tr_var1, tr_var2, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False 

        elif tr_type == "upgradeyamato":
            usr = get_planet_data(connection, tr_var1,"user")
            if usr == user:
                success = upgrade_yamato(connection, usr, tr_var1, tr_var2, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False 

        elif tr_type == "finishyamato":
            if virtualop:
                success = finish_yamato(connection, tr_var1,tr_var2,tr_var3, tr_var4, tr_var5, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "ask":
            if tr_var1 == "ship":          
                usr = get_ship_data(connection, tr_var2,"user")
            elif tr_var1 == "item":
                usr = get_item_data(connection, tr_var2,"owner")
            elif tr_var1 == "planet":
                usr = get_planet_data(connection, tr_var2,"user")
            else:
                usr = None
            if usr is not None and usr == user:
                success = ask(connection, usr, tr_var1, tr_var2, tr_var3, tr_var4, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False   

        elif tr_type == "cancel_ask":
            usr = get_ask_data(connection, tr_var1,"user")
            if usr == user:
                success = cancel_ask(connection, tr_var1, parameter, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False

        elif tr_type == "fill_ask":
            success = fill_ask(connection, user, tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(connection, success, id)

        elif tr_type == "buff":
            success = buff(connection, user, tr_var1, parameter, time_now, block_num, trx_id)
            update_transaction_status(connection, success, id)

        elif tr_type == "updatebuff":
            if user in ["nextcolony"]:
                success = updatebuff(connection, tr_var1, tr_var2, time_now, block_num, trx_id)
                update_transaction_status(connection, success, id)
            else:
                transaction_valid = False 

        else:
            transaction_valid = False

        delay_min = (datetime.now() - time_now).total_seconds() / 60
        delay_sec = int((datetime.now() - time_now).total_seconds())
        duration_sec = (time.time() - start_time)
        if delay_min < 1:
            print("%s (+ %d s): %s wants %s (%s, %s, %s)-> sucess: %s (dur. %.2f s)" % (str(time_now), delay_sec, user, tr_type, tr_var1, tr_var2, tr_var3, str(success), duration_sec))
        else:
            print("%s (+ %.1f min): %s wants %s (%s, %s, %s)-> sucess: %s (dur. %.2f s)" % (str(time_now), delay_min, user, tr_type, tr_var1, tr_var2, tr_var3, str(success), duration_sec))

        if not transaction_valid:
            update_transaction_status(connection, False, id)
    except Exception as e:
        raise e
    finally:
        print("process transaction " + trx_id)

# def get_transfer(trx):

#     id = trx['id']
#     user = trx['user']
#     memo = trx['memo']
#     trx_id = trx['trx']
#     block_num = trx['block_num']
#     time_now = trx['date']
#     amount = Amount(trx["amount"])
#     transfer_id = get_transfer_id()
#     at_symbol_pos = memo.find('@')
#     if at_symbol_pos < 0:
#         print("memo does not start with %s@: %s" % (transfer_id, memo))
#         update_transfer_status(False, id)
#         return False    
#     if len(memo) < at_symbol_pos + 1 or memo[:at_symbol_pos] != transfer_id:
#         print("memo does not start with %s@: %s" % (transfer_id, memo[:at_symbol_pos + 1]))
#         update_transfer_status(False, id)
#         return False
#     try:
#         data = json.loads(memo[at_symbol_pos + 1:])
#     except:
#         try:
#             data = ast.literal_eval(memo[at_symbol_pos + 1::])
#         except:
#             print("memo is not a json %s" % memo[at_symbol_pos + 1::])
#             update_transfer_status(False, id)
#             return False         
#     transfer_valid = True
#     if "command" not in data:
#         print("command not in data: %s" % str(data))
#         update_transfer_status(False, id)
#         return False            
#     if data["type"] == "auctionbid":
#         update_transfer_status(True, id)
#         return True
#     elif data["type"] == "buy":
#         command = data["command"]
#         if command["user"] == "":
#             command["user"] = user
#         success = buy(command, amount, time_now, block_num, trx_id, id)
#         print("%s: %s - %s -> sucess: %s" % (str(time_now), user, data["type"], str(success)))

def trigger_data():
    connection = connectdb()
    try:
        current_time = datetime.now()
        table = connection["virtualops"]
        parameter = read_parameter(connection)

        for trigger in table.find(tr_status=0, trigger_date={'<=': current_time}, order_by='trigger_date', _limit=10):
            connection.begin()
            try:
                table2 = connection["transactions"]  
                data = dict(trx=trigger["parent_trx"], user=trigger["user"], tr_type=trigger["tr_type"], tr_var1=trigger["tr_var1"],
                            tr_var2=trigger["tr_var2"], tr_var3=trigger["tr_var3"], tr_var4=trigger["tr_var4"], tr_var5=trigger["tr_var5"],
                            tr_var6=trigger["tr_var6"], tr_var7=trigger["tr_var7"], tr_var8=trigger["tr_var8"], tr_status=0 ,date=trigger["trigger_date"],
                            virtualop=1)
                table2.insert(data)
                table2 = connection["virtualops"] 
                table2.update({"id":trigger["id"], "tr_status": 1, "block_date": current_time}, ["id"])
                table2 = connection["transactions"]  
                trx = table2.find_one(trx=trigger["parent_trx"], tr_type=trigger["tr_type"])
                
                get_transaction(connection, trx, parameter)

                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
            
            
    finally:
        connection.close()

current_time = datetime.now()
print("현재 시간:", current_time)

trigger_data()
