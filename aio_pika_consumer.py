import json
import urllib, base64 , logging
from aio_pika import IncomingMessage,ExchangeType,connect
import os, sys, MySQLdb, random, time as t, threading
from datetime import datetime, timedelta
import asyncio , aiohttp, async_timeout,aiofiles
from kombu import Queue,Consumer,Producer,Exchange
from datetime import date, timedelta
from datetime import datetime
import nest_asyncio

#Degubber to point out Fault
import faulthandler
faulthandler.enable()

logging.basicConfig(filename='debugs.log',level=logging.DEBUG)


task_protocol = 1

acct_content = "application/json"

task_default_queue = 'SMSE'

task_queues = (

        Queue('SMSE', routing_key='SMSE',exchange='SMSE'),
        Queue('SMSU', routing_key='SMSU',exchange='SMSU'),
        Queue('SMSV', routing_key='SMSV',exchange='SMSV'),
        Queue('SERVER7', routing_key='SERVER7',exchange='SERVER7'),
)

#https://quentin.pradet.me/blog/how-do-you-limit-memory-usage-with-asyncio.html
#DO NOT INCREASE SEMAPHORE VALUE. MORE NUMBER WILL PUT MORE LOAD & MORE TRY CATCH HANDLING
MAX_SEMAPHORE = 100


HEADERS = {
        'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/45.0.2454.101 Safari/537.36'),
}

def fnWriteLogs( URLDetails,  ResponseText, ResponseCode, ResponseReason, TimeInterval , ProcessedTime):
        #print("Inside thread {}, Task Executed, Result : ".format(thread, str(Response['errors'])) )
        print("Result :" + str(ResponseText))

        currentDate = str(datetime.today().strftime('%Y%m%d'))

        fileName = "/var/www/html/celery/generatedlogs/"+str(URLDetails['server'])+"_processed_log_"+currentDate+".csv"
        #print(fileName)

        #Write the response to log file
        fH = open(fileName, "a+")
        fH.write( str(datetime.now())+','+str(URLDetails['username'])+','+ str(URLDetails['url'])+','+str(ResponseText)+','+str(ResponseReason)+','+str(ResponseCode)+','+str(TimeInterval)+'\n')
        fH.close()

async def fetch(URLDetails, session, timeout, startTime):
        with async_timeout.timeout(int(2)):
                if(URLDetails['method'] == 'POST'):
                        URLData = URLDetails['url'].split("?")
                        #print(URLData[0])
                        #print(URLData[1])
                        URLParams = {x[0] : x[1] for x in [x.split("=") for x in URLData[1][1:].split("&") ]}
                        async with session.post(URLDetails['url'], data=URLParams ) as response:
                                return await response.read(), response.status, response.reason , (t.time()-startTime)
                else:
                        async with session.get(URLDetails['url'] ) as response:
                                return await response.read(), response.status, response.reason , (t.time()-startTime)

async def fnProcessURL(loop, sem, URLDetails, session, timeout):
        #async with sem:
        async with aiohttp.ClientSession(loop=loop) as session:
                responseText, responseCode, responseReason, TimeInterval = await fetch(URLDetails, session, timeout, t.time())
                print(" Task Executed"  )
                fnWriteLogs( URLDetails, responseText, responseCode, responseReason, TimeInterval, t.time())

def on_messegee(message:IncomingMessage):
    with message.process():
        
        body=json.loads(message.body)
        # print(body)
        nest_asyncio.apply()
        # loop = asyncio.get_event_loop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        timeout = 2
        #session = aiohttp.ClientSession(loop=loop)
        session = ""

        # create instance of Semaphore
        sem = asyncio.Semaphore(MAX_SEMAPHORE)

        #body = json.loads(data)
        #print(body)
        logging.info('Destined URL : ' + str(body.get('URL')) )
        to_day = date.today().strftime('%d%m%Y')
        now = datetime.now()
        current_date = now.strftime("%d-%m-%Y")
        current_time = now.strftime("%H:%M:%S")

        #URL =  base64.decodestring(str(body.get('URL')))

        SERVER = str(body.get('SERVER'))
        #URL =  base64.decodestring(str(body.get('URL')))
        HTTP_METHOD = body.get('METHOD')
        USERNAME = str(body.get('USERNAME'))

        #URLDECODE THE DESINTED URL
        URL = str(body.get('URL'))
        #URL = urllib.parse.unquote(URL)
        #URL = urllib.parse.unquote_plus(URL)

        print("\nDestined URL : "+ URL)
        #sys.exit()

        URLDetails = {"url" : URL, "method" : HTTP_METHOD, "username" : USERNAME, "server" : SERVER}
        print("URLDetails : ",URLDetails)       
        # asyncio.run(fnProcessURL(sem, URLDetails, session,timeout ))

        #loop = asyncio.new_event_loop()
        loop.run_until_complete( fnProcessURL(loop, sem, URLDetails, session,timeout ))
        


# aio-pika cunsumer implimentation
async def main(loop):
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost:/", loop=loop)
    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    for queue in task_queues:
        queue_name=str(queue.name)
        queue_routingkey=str(queue.routing_key)
        queue_exchange=str(queue.name)

        # Declare an exchange
        # direct_logs_exchange = await channel.declare_exchange('logs', ExchangeType.DIRECT)

        # Declare queues
        queues = await channel.declare_queue(name=queue_name, auto_delete=False,durable=True)
        await queues.bind(routing_key=queue_routingkey,exchange=str(queue_exchange))

        #consuming queue messeges
        await queues.consume(on_messegee)
 
if __name__ == "__main__":

     loop = asyncio.get_event_loop()
     loop.create_task(main(loop))
     loop.run_forever()
