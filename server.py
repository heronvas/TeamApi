from flask import Flask, request
import pandas as pd
import numpy as np
import sqlite3
import sys
app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def hello_world():
    if request.method == 'POST':
        print("hello")
        
        try:
            sqliteConnection = sqlite3.connect('SoccerManagementSystem.db')
            cursor = sqliteConnection.cursor()
            data = cursor.execute("""SELECT name, country, startlink, endlink, year, p_id from player_details ;""").fetchall()
            date = cursor.execute("""SELECT Value from data_table where data_table.Field = ? ;""",("Updated_Date",)).fetchall()[0][0]
            keywords = ["summary", "passing", "misc", "defense", "possession"]
            team_ratings = pd.read_sql_query("Select * from team_ratings", sqliteConnection)
            mapping_value = dict(zip(team_ratings['Opponent'].values, team_ratings["Value"].values))
            update_date = ''
            for i in data[3:]:
                start = pd.DataFrame()
                for j in range(5):
                    org = pd.read_html(i[2]+"/"+i[4] +"/"+keywords[j]+i[3])
                    #print(i[2] +i[4] +"/"+keywords[j]+i[3])
                    #print(org[0])
                    temp = org[0]
                    #print(temp.columns)
                    temp.columns = temp.columns.get_level_values(1)
                    temp = pd.DataFrame(temp)
                    temp.drop(temp.tail(1).index, inplace = True)
                    temp = temp[temp['Squad'] != i[1]]
                    temp = temp[temp["Min"] != 'On matchday squad, but did not play']
                    temp = temp[temp['Date'].notna()]
                    temp = temp[(temp['Comp'] == 'Premier League') | (temp['Comp'] == 'La Liga') | (temp['Comp'] == 'Serie A') | (temp['Comp'] == 'Champions Lg') | (temp['Comp'] == 'Europa Lg')]
                    temp = temp.sort_values(["Date"], ascending = [False])
                    if(j == 0):
                        temp = temp[["Date", "Venue", "Opponent", "Pos", "Min", "Gls", "Ast", "Sh", "SoT", "CrdY", "CrdR", "Touches", "xG", "xA","Cmp%","Carries","Comp","Tkl"]]
                        temp["SoF"] = pd.to_numeric(temp["Sh"]) - pd.to_numeric(temp["SoT"])
                        temp = temp[temp["Touches"].notna()]
                        temp.set_index("Date", inplace=True)
                        temp.drop(["Sh"],axis = 1, inplace = True)
                    elif(j==1):
                        temp = temp[["Date", "KP"]]
                        temp.set_index("Date", inplace=True)
                    elif(j==2):
                        temp = temp[["Date","Off", "Fld", "PKwon", "PKcon", "OG","Won%"]]
                        temp.set_index("Date", inplace=True)
                    elif(j==3):
                        temp = temp[["Date","Int", "TklW"]]
                        #temp["TklW%"] = (temp["TklW"].values/temp["Tkl"].values)*100
                        temp.set_index("Date", inplace=True)
                        #temp.drop(["Tkl"],axis = 1, inplace = True)
                    elif(j==4):
                        temp = temp[["Date","Succ%"]]
                        temp.set_index("Date", inplace=True)
                    if(start.empty):
                        start = temp
                    else:
                        start = pd.merge(start, temp, how = "inner", left_index=True, right_index=True)
                        if(j==3):
                            start["TklW%"] = (pd.to_numeric(start["TklW"])/pd.to_numeric(start["Tkl"]))*100
                            start.drop(["Tkl", "TklW"],axis = 1, inplace = True)
                start.reset_index(inplace  = True)
                start['Date'] = start['Date'].apply(lambda x : x.replace('-','_'))
                start = start[start["Date"] > date]
                if(len(start)== 0):
                    return "No Value"
                start = start.fillna(0)
                start["Ratings"]= np.nan
                start["p_id"] = i[5]
                start = start.replace({"Opponent":mapping_value})
                update_date = start["Date"][0] if(start["Date"][0] > update_date) else update_date
                info = cursor.execute("PRAGMA table_info("+i[0]+");").fetchall()
                cn = []
                for n in info:
                    cn.append(n[1])
                change = start.pop("Ratings")
                start.insert(3, "Ratings", change)
                start.columns = cn
                print(start)
                start.to_sql(name = i[0], con = sqliteConnection, if_exists = "append", index = False) 
            cursor.execute("""UPDATE data_table set Value = ? where Field = ?;""",(update_date, "Updated_Date"))           
            sqliteConnection.commit()
            #print("done")
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
            return "Hello World err"
        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print("The SQLite connection is closed")
                #return "Hello World closed"

if __name__ == '__main__':
   app.run()


