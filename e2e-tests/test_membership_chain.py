#!/usr/bin/python3
import subprocess
import time
import requests

DYNAGO_CMD = "./dynago"
IP = "localhost"
PORT_A = 8081
A="localhost:8081"
PORT_B = 8085
B="localhost:8085"
PORT_C = 8087
C="localhost:8087"
PORT_D = 8089
D="localhost:8089"


HTTP_PORT_A = 8082
HTTP_PORT_B = 8086
HTTP_PORT_C = 8088
HTTP_PORT_D = 8090


BASE_URL_A = f"http://{IP}:{HTTP_PORT_A}/members"
BASE_URL_B = f"http://{IP}:{HTTP_PORT_B}/members"
BASE_URL_C = f"http://{IP}:{HTTP_PORT_C}/members"
BASE_URL_D = f"http://{IP}:{HTTP_PORT_D}/members"


servers = {}

def start_server(name, port, http_port, seed=None):
    cmd = [DYNAGO_CMD, "-i", IP, "-p", str(port), "-h", str(http_port)]
    if seed:
        cmd.extend(["-seed", seed])
    servers[name] = subprocess.Popen(cmd)
    time.sleep(2)

def stop_server(name):
    if name in servers:
        servers[name].terminate()
        servers[name].wait()
        del servers[name]
        time.sleep(2)

def get_cluster_members(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting cluster members from {url}: {e}")
        return set()

def test_dynago_cluster():
    try:
        # Step 1: Start Server A
        start_server(A, PORT_A, HTTP_PORT_A)

        # Step 2: Start Server B pointing to A
        start_server(B, PORT_B, HTTP_PORT_B, f"{IP}:{PORT_A}")

        # Step 3: Start Server C pointing to B
        start_server(C, PORT_C, HTTP_PORT_C, f"{IP}:{PORT_B}")

        # Step 4: Start Server D pointing to C
        start_server(D, PORT_D, HTTP_PORT_D, f"{IP}:{PORT_C}")
        time.sleep(20)
        # Step 4: Check initial cluster membership
        expected_members = {A, B, C, D}
        print(set(expected_members))
        print(set(get_cluster_members(BASE_URL_A)))
        assert set(get_cluster_members(BASE_URL_A)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_B)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_C)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_D)) == set(expected_members)

        stop_server(D)
        expected_members = {A, B, C}
        print("Stopped D. Waiting for cluster info to propagate")
        time.sleep(25)  # Allow cluster to update
        print(set(expected_members))
        print(set(get_cluster_members(BASE_URL_A)))
        assert set(get_cluster_members(BASE_URL_A)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_B)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_C)) == set(expected_members)
    

        # Restart Server D
        start_server(D, PORT_D, HTTP_PORT_D, f"{IP}:{PORT_C}")

        print("Restarted the server. Waiting for cluster info to propagate")
        time.sleep(20)
        expected_members = {A, B, C, D}
        print(set(expected_members))
        print(set(get_cluster_members(BASE_URL_A)))
        assert set(get_cluster_members(BASE_URL_A)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_B)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_C)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_D)) == set(expected_members)

        # Stop Server B
        
        stop_server(B)
        expected_members_A = {A}
        expected_members_CD = {C,D}
        print("Stopped B. Waiting for cluster info to propagate")
        time.sleep(25)  # Allow cluster to update
        assert set(get_cluster_members(BASE_URL_A)) == set(expected_members_A)
        assert set(get_cluster_members(BASE_URL_C)) == set(expected_members_CD)
        assert set(get_cluster_members(BASE_URL_D)) == set(expected_members_CD)

        # Restart Server B
        start_server(B, PORT_B, HTTP_PORT_B, f"{IP}:{PORT_A}")

        print("Restarted the server. Waiting for cluster info to propagate")
        time.sleep(20)
        expected_members = {A, B, C, D}
        print(set(expected_members))
        print(set(get_cluster_members(BASE_URL_A)))
        assert set(get_cluster_members(BASE_URL_A)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_B)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_C)) == set(expected_members)
        assert set(get_cluster_members(BASE_URL_D)) == set(expected_members)
    
        print("Test passed!")

    finally:
        # Cleanup
        for server_name in list(servers.keys()):
            stop_server(server_name)

if __name__ == '__main__':
    test_dynago_cluster()
