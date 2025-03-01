import subprocess
import time
import requests

DYNAGO_CMD = "./dynago"
IP = "localhost"
PORT_A = 8081
PORT_B = 8085
PORT_C = 8087
HTTP_PORT_A = 8082
HTTP_PORT_B = 8086
HTTP_PORT_C = 8088

BASE_URL_A = f"http://{IP}:{HTTP_PORT_A}/cluster/members"
BASE_URL_B = f"http://{IP}:{HTTP_PORT_B}/cluster/members"
BASE_URL_C = f"http://{IP}:{HTTP_PORT_C}/cluster/members"

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
        return set(response.json().get("members", []))
    except Exception as e:
        print(f"Error getting cluster members from {url}: {e}")
        return set()

def test_dynago_cluster():
    try:
        # Step 1: Start Server A
        start_server("A", PORT_A, HTTP_PORT_A)

        # Step 2: Start Server B pointing to A
        start_server("B", PORT_B, HTTP_PORT_B, f"{IP}:{PORT_A}")

        # Step 3: Start Server C pointing to A
        start_server("C", PORT_C, HTTP_PORT_C, f"{IP}:{PORT_A}")

        # Step 4: Check initial cluster membership
        expected_members = {"A", "B", "C"}
        assert get_cluster_members(BASE_URL_A) == expected_members
        assert get_cluster_members(BASE_URL_B) == expected_members
        assert get_cluster_members(BASE_URL_C) == expected_members

        # Step 5: Kill Server B
        stop_server("B")
        expected_members = {"A", "C"}
        time.sleep(2)  # Allow cluster to update
        assert get_cluster_members(BASE_URL_A) == expected_members
        assert get_cluster_members(BASE_URL_C) == expected_members

        # Step 6: Restart Server B
        start_server("B", PORT_B, HTTP_PORT_B, f"{IP}:{PORT_A}")

        # Step 7: Check all 3 servers for updated members
        expected_members = {"A", "B", "C"}
        assert get_cluster_members(BASE_URL_A) == expected_members
        assert get_cluster_members(BASE_URL_B) == expected_members
        assert get_cluster_members(BASE_URL_C) == expected_members

        print("Test passed!")

    finally:
        # Cleanup
        for server_name in list(servers.keys()):
            stop_server(server_name)

if __name__ == '__main__':
    test_dynago_cluster()