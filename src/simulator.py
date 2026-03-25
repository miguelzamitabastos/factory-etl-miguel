import json
import time
import random
from datetime import datetime
# When using this module as part of the src package we perform a relative import
# so that Python can resolve kafka_client regardless of the current working directory.
from utils.kafka_client import create_producer
import os
from dotenv import load_dotenv
import json

load_dotenv()

raw_topic = os.getenv("KAFKA_RAW_TOPIC")
producer = create_producer()

# Factory machines
MACHINES = [
    "UNS56A",
    "WS964F",
    "IS8710",
    "FB713A",
    "C7841R",
    "CPM784",
    "LBL74F",
    "PLL741",
    "DF784W",
    "256FG9"
]

MACHINE_PROPERTIES = {
    "UNS56A": lambda : {},
    "WS964F": lambda : {'A7':random.randint(0,25)},
    "IS8710": lambda : {'W8':random.randint(0,5),'L1':random.randint(0,10)},
    "FB713A": lambda : {'T3':random.randint(0,60)},
    "C7841R": lambda : {'T3':random.randint(0,60)},
    "CPM784": lambda : {'P6':random.randint(0,10)},
    "LBL74F": lambda : {'G8':random.randint(0,180)},
    "PLL741": lambda : {'T3':random.randint(0,60)},
    "DF784W": lambda : {},
    "256FG9": lambda : {}
}

product_positions = []
product_index = 1

def produce_event(event):
        producer.send(raw_topic, event)

def generate_event(product_id, position_index):
    return {
        "TS": datetime.now().isoformat(),
        "PR": product_id,
        "MC": MACHINES[position_index],
        "PS": MACHINE_PROPERTIES[MACHINES[position_index]]()
    }

def create_position():
    global product_index
    position = {
        "index":0,
        "product_id": product_index,
    }
    product_index += 1
    return position

print("To stop the simulation press Ctrl+C")



try:
    while True:
        if len(product_positions) == 0:
            position = create_position()
            event = generate_event(product_id=position['product_id'], position_index=position['index'])
            product_positions.append(position)
            produce_event(event)
            print(position)
        else:
            for position in product_positions[::-1]:
                if position['index'] == len(MACHINES)-1:
                    product_positions.pop(len(MACHINES)-1)
                else :
                    position['index'] += 1
                    event = generate_event(product_id=position['product_id'], position_index=position['index'])
                    produce_event(event)
                    print(position)
                time.sleep(1)
                
            if len(product_positions) < 10:
                position = create_position()
                event = generate_event(product_id=position['product_id'], position_index=position['index'])
                produce_event(event)
                product_positions.insert(0,position)
                print(position)

        print('----')
        time.sleep(3)
except Exception as e:
    print(e)
    print("\nSimulation Stopped")

finally:
    producer.flush()
    producer.close()
