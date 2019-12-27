import json
import datetime
from azure.eventhub import EventHubClientAsync, Sender, EventData
import asyncio
from sys import getsizeof


message = {"Menssagem"}
message = json.dumps(message, default=str)
message_size = getsizeof(message)

count = 1000
messages_list = []
event_tasks = 20

list_chunck_size = int(count/event_tasks)

for i in range(count):
    messages_list.append(message)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

splited_message_list = list(chunks(messages_list, list_chunck_size))

initial_time = datetime.datetime.now()
ADDRESS = "EventHubsAdress"
USER = ""
KEY = ""

# Create Event Hubs client
client = EventHubClientAsync(ADDRESS, debug=True, username=USER, password=KEY)
sender = client.add_async_sender()


async def run(client, message_event):
    sender = client.add_async_sender()
    await client.run_async()
    await send(sender, message_event)

async def send(snd, messages):
    count_execution = 0
    for i in messages:
        count_execution = count_execution + 1
        data = EventData(str(i))
        await snd.send(data)

loop = asyncio.get_event_loop()
tasks = asyncio.gather(
        *[run(client, message) for message in splited_message_list])
loop.run_until_complete(tasks)
loop.run_until_complete(client.stop_async())
loop.close()


total_bytes = message_size*count
lastdate = datetime.datetime.now()
total_time = (lastdate - initial_time).total_seconds()
print("Tempo Total: " + str(total_time))
print("Mensagens por Segundo: " + str(count/total_time) )
print("Total de Mensagens: " + str(count))
print("Total ddo Tamanho das Mensagens em MB: " + str( ((total_bytes)/1024)/1024 ) )
print("KiloBytes por Segundo: " + str((total_bytes/total_time)/1024))
