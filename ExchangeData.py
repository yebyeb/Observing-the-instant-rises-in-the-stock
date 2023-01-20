import ccxt
import pandas as pd
import time
#from datetime import datetime
import mysql.connector

mydb = mysql.connector.connect(
  host="",
  user="",
  password="",
  database=""
)

mycursor = mydb.cursor()



indexNumbUSDT = []
indexNumbBUSD = []

indexNumbFutureUSDT = []
indexNumbFutureBUSD = []

dataSUSDT1 = ("S",1,1,1)
dataSUSDT2 = ("S",1,1,1)
dataSUSDT3 = ("S",1,1,1)

dataSBUSD1 = ("S",1,1,1)
dataSBUSD2 = ("S",1,1,1)
dataSBUSD3 = ("S",1,1,1)

dataFUSDT1 = ("S",1,1,1)
dataFUSDT2 = ("S",1,1,1)
dataFUSDT3 = ("S",1,1,1)

dataFBUSD1 = ("S",1,1,1)
dataFBUSD2 = ("S",1,1,1)
dataFBUSD3 = ("S",1,1,1)

priceVievMin = 2

counterOver = 46

counter = 0

formulaBool = False

while True:
    try:

      if counter == counterOver:
       counter = 0

      # SPOT

      data = pd.read_json('https://api.binance.com/api/v3/ticker/24hr')



      #btcPriceAcc = data['lastPrice'][data[data['symbol'] == 'BTCUSDT'].index[0]]
      # print(btcPriceAcc,PriceStartPers,VolStartPers,PriceStartPersSell,VolStartPersSell,VolStartMinPersSell,LongTakeProfitPers,LongStopLossPers,SortTakeProfitPers,ShortStopLossPers)


      dataSpot1 = pd.DataFrame(data.sort_values(by=["symbol"], ascending=True), columns=["symbol","lastPrice"])
      dataSpot1.drop(dataSpot1[dataSpot1["lastPrice"] == 0].index, inplace = True)

      dataSpotUSDT = dataSpot1[dataSpot1['symbol'].str.endswith('USDT')]
      dataSpotBUSD = dataSpot1[dataSpot1['symbol'].str.endswith('BUSD')]
      #dataSpotBTC = dataSpot1[dataSpot1['symbol'].str.endswith('BTC')]
      #dataSpotETH = dataSpot1[dataSpot1['symbol'].str.endswith('ETH')]
      #dataSpotBNB = dataSpot1[dataSpot1['symbol'].str.endswith('BNB')]

      time.sleep(0.01)

      # sUSDTC
      if counter == 0 :

        SdataUSDT = dataSpotUSDT["symbol"].values.tolist()
        PdataUSDT = dataSpotUSDT["lastPrice"].values.tolist()

        indexNumbUSDT.clear()
        for x in range(len(dataSpotUSDT)):
         indexNumbUSDT.append(x)
      if counter != 0:
        PdataUSDT1 = dataSpotUSDT["lastPrice"].values.tolist()
        PdataUSDT2 = [i - j for i, j in zip(PdataUSDT1, PdataUSDT)]
        PdataUSDT3 = [i / j for i, j in zip(PdataUSDT2, PdataUSDT)]
        PdataUSDTACC =  [i * 100 for i in PdataUSDT3]

        #table1
        tableUSDT0 = pd.DataFrame(list(zip(SdataUSDT,PdataUSDTACC,PdataUSDT1)),columns =['Symbol','priceAcc','price'])
        tableUSDT0.sort_values(by=['priceAcc'], inplace=True, ascending=False)

        tableUSDT0['IndexNumb'] = indexNumbUSDT
        tableUSDT00 = tableUSDT0.set_index('IndexNumb')

        symbolUSDT = tableUSDT00['Symbol'][0]
        priceAccUSDT = tableUSDT00['priceAcc'][0]
        priceUSDT = format(tableUSDT00['price'][0], '.8f')

        dataSUSDT1 = (symbolUSDT,str(priceUSDT),str(priceAccUSDT),"SUSDT1")

        symbolUSDT2 = tableUSDT00['Symbol'][1]
        priceAccUSDT2 = tableUSDT00['priceAcc'][1]
        priceUSDT2 = format(tableUSDT00['price'][1], '.8f')

        dataSUSDT2 = (symbolUSDT2,str(priceUSDT2),str(priceAccUSDT2),"SUSDT2")

        symbolUSDT3 = tableUSDT00['Symbol'][2]
        priceAccUSDT3 = tableUSDT00['priceAcc'][2]
        priceUSDT3 = format(tableUSDT00['price'][2], '.8f')

        dataSUSDT3 = (symbolUSDT3,str(priceUSDT3),str(priceAccUSDT3),"SUSDT3")


      time.sleep(0.01)

      # sBUSDC
      if counter == 0 :

        SdataBUSD = dataSpotBUSD["symbol"].values.tolist()
        PdataBUSD = dataSpotBUSD["lastPrice"].values.tolist()

        indexNumbBUSD.clear()
        for x in range(len(dataSpotBUSD)):
         indexNumbBUSD.append(x)
      if counter != 0:
        PdataBUSD1 = dataSpotBUSD["lastPrice"].values.tolist()
        PdataBUSD2 = [i - j for i, j in zip(PdataBUSD1, PdataBUSD)]
        PdataBUSD3 = [i / j for i, j in zip(PdataBUSD2, PdataBUSD)]
        PdataBUSDACC =  [i * 100 for i in PdataBUSD3]

        #table1
        tableBUSD0 = pd.DataFrame(list(zip(SdataBUSD,PdataBUSDACC,PdataBUSD1)),columns =['Symbol','priceAcc','price'])
        tableBUSD0.sort_values(by=['priceAcc'], inplace=True, ascending=False)

        tableBUSD0['IndexNumb'] = indexNumbBUSD
        tableBUSD00 = tableBUSD0.set_index('IndexNumb')

        symbolBUSD = tableBUSD00['Symbol'][0]
        priceAccBUSD = tableBUSD00['priceAcc'][0]
        priceBUSD = format(tableBUSD00['price'][0], '.8f')

        dataSBUSD1 = (symbolBUSD,str(priceBUSD),str(priceAccBUSD),"SBUSD1")

        symbolBUSD2 = tableBUSD00['Symbol'][1]
        priceAccBUSD2 = tableBUSD00['priceAcc'][1]
        priceBUSD2 = format(tableBUSD00['price'][1], '.8f')

        dataSBUSD2 = (symbolBUSD2,str(priceBUSD2),str(priceAccBUSD2),"SBUSD2")

        symbolBUSD3 = tableBUSD00['Symbol'][2]
        priceAccBUSD3 = tableBUSD00['priceAcc'][2]
        priceBUSD3 = format(tableBUSD00['price'][2], '.8f')

        dataSBUSD3 = (symbolBUSD3,str(priceBUSD3),str(priceAccBUSD3),"SBUSD3")

      time.sleep(1.39)

      # PERPETUAL

      dataFuture = pd.read_json('https://fapi.binance.com/fapi/v1/ticker/24hr')

      dataFuture1 = pd.DataFrame(dataFuture.sort_values(by=["symbol"], ascending=True), columns=["symbol","lastPrice"])
      dataFuture1.drop(dataFuture1[dataFuture1["lastPrice"] == 0].index, inplace = True)

      dataFutureUSDT = dataFuture1[dataFuture1['symbol'].str.endswith('USDT')] # USDⓈ - M Perpetual
      dataFutureBUSD = dataFuture1[dataFuture1['symbol'].str.endswith('BUSD')] # BUSD - M Perpetual

      # sFutureUSDTC
      if counter == 0 :

        SdataFutureUSDT = dataFutureUSDT["symbol"].values.tolist()
        PdataFutureUSDT = dataFutureUSDT["lastPrice"].values.tolist()

        indexNumbFutureUSDT.clear()
        for x in range(len(dataFutureUSDT)):
         indexNumbFutureUSDT.append(x)
      if counter != 0:
        PdataFutureUSDT1 = dataFutureUSDT["lastPrice"].values.tolist()
        PdataFutureUSDT2 = [i - j for i, j in zip(PdataFutureUSDT1, PdataFutureUSDT)]
        PdataFutureUSDT3 = [i / j for i, j in zip(PdataFutureUSDT2, PdataFutureUSDT)]
        PdataFutureUSDTACC =  [i * 100 for i in PdataFutureUSDT3]

        #table1
        tableFutureUSDT0 = pd.DataFrame(list(zip(SdataFutureUSDT,PdataFutureUSDTACC,PdataFutureUSDT1)),columns =['Symbol','priceAcc','price'])
        tableFutureUSDT0.sort_values(by=['priceAcc'], inplace=True, ascending=False)

        tableFutureUSDT0['IndexNumb'] = indexNumbFutureUSDT
        tableFutureUSDT00 = tableFutureUSDT0.set_index('IndexNumb')

        symbolFutureUSDT = tableFutureUSDT00['Symbol'][0]
        priceAccFutureUSDT = tableFutureUSDT00['priceAcc'][0]
        priceFutureUSDT = format(tableFutureUSDT00['price'][0], '.8f')

        dataFUSDT1 = (symbolFutureUSDT,str(priceFutureUSDT),str(priceAccFutureUSDT),"FUSDT1")

        symbolFutureUSDT2 = tableFutureUSDT00['Symbol'][1]
        priceAccFutureUSDT2 = tableFutureUSDT00['priceAcc'][1]
        priceFutureUSDT2 = format(tableFutureUSDT00['price'][1], '.8f')

        dataFUSDT2 = (symbolFutureUSDT2,str(priceFutureUSDT2),str(priceAccFutureUSDT2),"FUSDT2")

        symbolFutureUSDT3 = tableFutureUSDT00['Symbol'][2]
        priceAccFutureUSDT3 = tableFutureUSDT00['priceAcc'][2]
        priceFutureUSDT3 = format(tableFutureUSDT00['price'][2], '.8f')

        dataFUSDT3 = (symbolFutureUSDT3,str(priceFutureUSDT3),str(priceAccFutureUSDT3),"FUSDT3")

      time.sleep(0.01)

      # sFutureBUSDC
      if counter == 0 :

        SdataFutureBUSD = dataFutureBUSD["symbol"].values.tolist()
        PdataFutureBUSD = dataFutureBUSD["lastPrice"].values.tolist()

        indexNumbFutureBUSD.clear()
        for x in range(len(dataFutureBUSD)):
         indexNumbFutureBUSD.append(x)
      if counter != 0:
        PdataFutureBUSD1 = dataFutureBUSD["lastPrice"].values.tolist()
        PdataFutureBUSD2 = [i - j for i, j in zip(PdataFutureBUSD1, PdataFutureBUSD)]
        PdataFutureBUSD3 = [i / j for i, j in zip(PdataFutureBUSD2, PdataFutureBUSD)]
        PdataFutureBUSDACC =  [i * 100 for i in PdataFutureBUSD3]

        #table1
        tableFutureBUSD0 = pd.DataFrame(list(zip(SdataFutureBUSD,PdataFutureBUSDACC,PdataFutureBUSD1)),columns =['Symbol','priceAcc','price'])
        tableFutureBUSD0.sort_values(by=['priceAcc'], inplace=True, ascending=False)

        tableFutureBUSD0['IndexNumb'] = indexNumbFutureBUSD
        tableFutureBUSD00 = tableFutureBUSD0.set_index('IndexNumb')

        symbolFutureBUSD = tableFutureBUSD00['Symbol'][0]
        priceAccFutureBUSD = tableFutureBUSD00['priceAcc'][0]
        priceFutureBUSD = format(tableFutureBUSD00['price'][0], '.8f')

        dataFBUSD1 = (symbolFutureBUSD,str(priceFutureBUSD),str(priceAccFutureBUSD),"FBUSD1")

        symbolFutureBUSD2 = tableFutureBUSD00['Symbol'][1]
        priceAccFutureBUSD2 = tableFutureBUSD00['priceAcc'][1]
        priceFutureBUSD2 = format(tableFutureBUSD00['price'][1], '.8f')

        dataFBUSD2 = (symbolFutureBUSD2,str(priceFutureBUSD2),str(priceAccFutureBUSD2),"FBUSD2")

        symbolFutureBUSD3 = tableFutureBUSD00['Symbol'][2]
        priceAccFutureBUSD3 = tableFutureBUSD00['priceAcc'][2]
        priceFutureBUSD3 = format(tableFutureBUSD00['price'][2], '.8f')

        dataFBUSD3 = (symbolFutureBUSD3,str(priceFutureBUSD3),str(priceAccFutureBUSD3),"FBUSD3")

      if counter > 0:
       sql = """Update Formulas set Symbol = %s, Price = %s , PriceAcc = %s where formulaType = %s"""

       mycursor.execute(sql, dataSUSDT1)
       mycursor.execute(sql, dataSUSDT2)
       mycursor.execute(sql, dataSUSDT3)
       mycursor.execute(sql, dataSBUSD1)
       mycursor.execute(sql, dataSBUSD2)
       mycursor.execute(sql, dataSBUSD3)
       mycursor.execute(sql, dataFUSDT1)
       mycursor.execute(sql, dataFUSDT2)
       mycursor.execute(sql, dataFUSDT3)
       mycursor.execute(sql, dataFBUSD1)
       mycursor.execute(sql, dataFBUSD2)
       mycursor.execute(sql, dataFBUSD3)

       mydb.commit()
       print("Multiple columns updated successfully ")

       ## defining the Query
       query = "SELECT * FROM Formulas"

       ## getting records from the table
       mycursor.execute(query)

       ## fetching all records from the 'cursor' object
       records = mycursor.fetchall()

       ## Showing the data
       for record in records:
         print(record)

       #if connection.is_connected():
         #connection.close()


      time.sleep(0.01)
      counter = counter + 1

    except ccxt.BaseError as Error:
         print ("[ERROR] ", Error )
         continue
    except mysql.connector.Error as error:
         print("Failed to update columns of table: {}".format(error))
         continue
