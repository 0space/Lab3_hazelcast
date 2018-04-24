#!/usr/bin/python2

import logging
import hazelcast

def config_client(server_list):
    config = hazelcast.ClientConfig()
    # Hazelcast.Address is the hostname or IP address, e.g. 'localhost:5701'
    for server in server_list:
        config.network_config.addresses.append(server)
    # basic logging setup to see client logs
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    return config

def map_create(client):
    map = client.get_map("products")
    map.add_index("value" , True)

def map_show(client):
    map = client.get_map("products").blocking()
    print("map.size", map.size())
    # print("map.get", map.key_set())
    for key in map.key_set():
        print("{} :: {}".format(key, map.get(key)))

def map_add(client, data_list):
    transaction_list = list()
    for key in data_list:
        t = client.new_transaction(100, 3, hazelcast.transaction.TWO_PHASE)
        value = data_list[key]
        transaction_list.append(t)
        t.begin()
        map = t.get_map("products")
        map.put(key, value)
        try:
            t.commit()
        except:
            print("Transactions failed")
            for i in transaction_list:
                print("Rollback")
                i.rollback()
                return False
    return True
    # for key in data_list:
    #     value = data_list[key]
    #     map = t.get_map("products")
    #     if map.contains_key(key):
    #         print("Rollback")
    #         t.rollback()
    #         return False
    #     map.put(key, value)
    #     print("Commit")
    # t.commit()
    # return True

def commit_change(client):
    # tm = hazelcast.transaction
    # tm.commit()
    t = client.new_transaction(100, 3, '~hazelcast.transaction.ONE_PHASE')
    t.begin()
    t.commit()

def main():
    hosts = ['localhost:5701', 'localhost:5702', 'localhost:5703']
    config = config_client(hosts)
    client = hazelcast.HazelcastClient(config)
    map_create(client)
    map_add(client, {'IPhone X': 899})
    # map_add(client, 'IPhone 8', 799)
    # map_add(client, 'Galaxy S9+', 899)
    # map_add(client, '1', 1)
    # map_add(client, '2', 2)
    # map_add(client, '3', 3)
    # map_add(client, '4', 4)
    # map_add(client, '5', 5)
    map_show(client)
    client.shutdown()

if __name__ == "__main__":
    main()
