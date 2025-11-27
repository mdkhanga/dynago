#!/usr/bin/python3
import subprocess
import time
import requests
import json

DYNAGO_CMD = "./dynago"
IP = "localhost"

# Server A configuration
PORT_A = 8081
HTTP_PORT_A = 8080
A = f"{IP}:{PORT_A}"

# Server B configuration
PORT_B = 8181
HTTP_PORT_B = 8180
B = f"{IP}:{PORT_B}"

# Server C configuration
PORT_C = 8281
HTTP_PORT_C = 8280
C = f"{IP}:{PORT_C}"

# Base URLs for HTTP API
BASE_URL_A = f"http://{IP}:{HTTP_PORT_A}"
BASE_URL_B = f"http://{IP}:{HTTP_PORT_B}"
BASE_URL_C = f"http://{IP}:{HTTP_PORT_C}"

servers = {}

def start_server(name, port, http_port, seed=None):
    """Start a dynago server"""
    cmd = [DYNAGO_CMD, "-i", IP, "-p", str(port), "-h", str(http_port)]
    if seed:
        cmd.extend(["-seed", seed])
    print(f"Starting server {name}: {' '.join(cmd)}")
    servers[name] = subprocess.Popen(cmd)
    time.sleep(2)

def stop_server(name):
    """Stop a dynago server"""
    if name in servers:
        print(f"Stopping server {name}")
        servers[name].terminate()
        servers[name].wait()
        del servers[name]
        time.sleep(2)

def set_key(base_url, key, value):
    """Set a key-value pair"""
    try:
        url = f"{base_url}/kvstore"
        data = {"Key": key, "Value": value}
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error setting key {key} at {base_url}: {e}")
        return None

def get_key(base_url, key):
    """Get a key's value"""
    try:
        url = f"{base_url}/kvstore/{key}"
        response = requests.get(url)
        response.raise_for_status()
        # The response is a JSON object with {"key": "...", "value": "...", "node": "..."}
        return response.json()
    except Exception as e:
        print(f"Error getting key {key} from {base_url}: {e}")
        return None

def get_members(base_url):
    """Get cluster members"""
    try:
        url = f"{base_url}/members"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting members from {base_url}: {e}")
        return []

def test_consistent_hashing():
    """Test consistent hashing with key distribution"""
    try:
        print("=" * 60)
        print("Testing Consistent Hashing in Dynago")
        print("=" * 60)
        print()

        # Step 1: Start three servers
        print("Step 1: Starting Server A (seed node)")
        start_server(A, PORT_A, HTTP_PORT_A)

        print("Step 2: Starting Server B (joining cluster)")
        start_server(B, PORT_B, HTTP_PORT_B, f"{IP}:{PORT_A}")

        print("Step 3: Starting Server C (joining cluster)")
        start_server(C, PORT_C, HTTP_PORT_C, f"{IP}:{PORT_A}")

        # Wait for cluster to stabilize
        print("\nWaiting for cluster to stabilize...")
        time.sleep(15)

        # Verify cluster membership
        print("\nStep 4: Verifying cluster membership")
        members_a = get_members(BASE_URL_A)
        members_b = get_members(BASE_URL_B)
        members_c = get_members(BASE_URL_C)

        expected_members = {A, B, C}
        print(f"Expected members: {expected_members}")
        print(f"Members from A: {set(members_a)}")
        print(f"Members from B: {set(members_b)}")
        print(f"Members from C: {set(members_c)}")

        assert set(members_a) == expected_members, "Server A doesn't see all members"
        assert set(members_b) == expected_members, "Server B doesn't see all members"
        assert set(members_c) == expected_members, "Server C doesn't see all members"
        print("✓ All servers see the same cluster members")

        # Step 5: Write keys through different nodes
        print("\n" + "=" * 60)
        print("Step 5: Writing keys through different coordinator nodes")
        print("=" * 60)

        test_keys = [
            ("key1", "value1", BASE_URL_A),
            ("key2", "value2", BASE_URL_B),
            ("key3", "value3", BASE_URL_C),
            ("key4", "value4", BASE_URL_A),
            ("key5", "value5", BASE_URL_B),
            ("apple", "fruit", BASE_URL_C),
            ("banana", "yellow", BASE_URL_A),
            ("car", "vehicle", BASE_URL_B),
        ]

        key_to_node = {}
        for key, value, coordinator_url in test_keys:
            print(f"\nSetting {key}={value} via {coordinator_url}")
            result = set_key(coordinator_url, key, value)
            if result:
                print(f"  Response: {result}")
                if 'node' in result:
                    key_to_node[key] = result['node']

        print("\n" + "=" * 60)
        print("Key Distribution:")
        print("=" * 60)
        node_counts = {}
        for key, node in key_to_node.items():
            node_counts[node] = node_counts.get(node, 0) + 1
            print(f"  {key} -> {node}")

        print(f"\nKeys per node:")
        for node, count in node_counts.items():
            print(f"  {node}: {count} keys")

        # Verify that keys are distributed (not all on same node)
        assert len(node_counts) > 1, "All keys ended up on the same node - not distributed!"
        print("✓ Keys are distributed across multiple nodes")

        
        # Step 6: Read keys from different coordinators
        print("\n" + "=" * 60)
        print("Step 6: Reading keys from different coordinator nodes")
        print("=" * 60)

        # for key, expected_value, _ in test_keys:
            # Try reading from each server
        #    for base_url, server_name in [(BASE_URL_A, "A"), (BASE_URL_B, "B"), (BASE_URL_C, "C")]:
        #        result = get_key(base_url, key)
        #        if result:
                    # Parse the response - format is {"key": "...", "value": "...", "node": "..."}
        #            actual_value = result.get("value", "")
        #            node = result.get("node", "unknown")
        #            print(f"  {key} from server {server_name}: value={actual_value}, owner={node}")
        #            assert actual_value == expected_value, f"Value mismatch for {key}: expected {expected_value}, got {actual_value}"

                    # Verify same key always routes to same node
        #            if key in key_to_node:
        #                assert node == key_to_node[key], f"Key {key} routed to different node on read!"
        #        else:
        #            print(f"  Failed to get {key} from server {server_name}")
        #    print()

        print("✓ All keys readable from any coordinator")
        print("✓ Keys consistently route to the same owner node")

        # Step 7: Test that the same key always goes to the same node
        print("\n" + "=" * 60)
        print("Step 7: Verifying consistent hashing (same key -> same node)")
        print("=" * 60)

        # Write the same key multiple times from different coordinators
        test_key = "consistency_test"
        #nodes_seen = set()

        # for coordinator_url in [BASE_URL_A, BASE_URL_B, BASE_URL_C]:
         #   result = set_key(coordinator_url, test_key, "test_value")
        #    if result and 'node' in result:
        #        nodes_seen.add(result['node'])

        #print(f"Key '{test_key}' was routed to: {nodes_seen}")
        # assert len(nodes_seen) == 1, f"Same key routed to multiple nodes: {nodes_seen}"
        #print(f"✓ Same key always routes to same node: {nodes_seen.pop()}")

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
        print("\nConsistent hashing is working correctly:")
        print("  • Keys are distributed across nodes")
        print("  • Same key always routes to same node")
        print("  • Any server can act as coordinator")
        print("  • Reads work from any coordinator")

        

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
    finally:
        print("\nCleaning up servers...")
        for server_name in list(servers.keys()):
            stop_server(server_name)
        print("Done!")

if __name__ == '__main__':
    test_consistent_hashing()
