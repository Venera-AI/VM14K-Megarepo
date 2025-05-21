import multiprocessing
import time


def put_data(shared_list):
    # Add a dictionary to the shared list
    shared_list.append({"status": "done", "message": "1".encode()})
    shared_list.append({"status": "done", "message": "2".encode()})
    shared_list.append({"status": "done", "message": "3".encode()})
    time.sleep(10)


def get_data(shared_list, data):
    # Get a message from the queue
    message = shared_list.pop(0)
    print(f"Message from process: {message}")
    message = shared_list.pop(0)
    print(f"Message from process: {message}")
    time.sleep(10)


manager = multiprocessing.Manager()
shared_list = manager.list()
# Push values 1, 2, 3
p1 = multiprocessing.Process(target=put_data, args=(shared_list,))
p2 = multiprocessing.Process(target=get_data, args=(shared_list,))
p1.start()
p2.start()
p1.join()
p2.join()
