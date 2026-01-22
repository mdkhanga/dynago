#!/usr/bin/python3
import subprocess
import time
import requests
import json
import random
import string

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

def generate_random_key(length=10):
    """Generate a random key"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def test_key_distribution():
    """Test that 1000 keys are evenly distributed across 3 nodes"""
    try:
        print("=" * 60)
        print("Testing Key Distribution in Dynago")
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

        # Step 5: Send 1000 requests with random keys
        print("\n" + "=" * 60)
        print("Step 5: Sending 1000 requests with random keys")
        print("=" * 60)

        NUM_KEYS = 10000
        coordinator_urls = [BASE_URL_A, BASE_URL_B, BASE_URL_C]
        node_counts = {}
        failed_requests = 0

        for i in range(NUM_KEYS):
            key = generate_random_key()
            value = f"value_{i}"
            # Round-robin across coordinators
            coordinator_url = coordinator_urls[i % 3]

            result = set_key(coordinator_url, key, value)
            if result and 'node' in result:
                node = result['node']
                node_counts[node] = node_counts.get(node, 0) + 1
            else:
                failed_requests += 1

            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"  Sent {i + 1}/{NUM_KEYS} requests...")

        print(f"\nCompleted sending {NUM_KEYS} requests")
        print(f"Failed requests: {failed_requests}")

        # Step 6: Analyze distribution
        print("\n" + "=" * 60)
        print("Step 6: Analyzing key distribution")
        print("=" * 60)

        total_successful = sum(node_counts.values())
        print(f"\nTotal successful requests: {total_successful}")
        print(f"\nKeys per node:")

        for node in sorted(node_counts.keys()):
            count = node_counts[node]
            percentage = (count / total_successful) * 100
            print(f"  {node}: {count} keys ({percentage:.1f}%)")

        # Step 7: Check for even distribution
        print("\n" + "=" * 60)
        print("Step 7: Verifying even distribution")
        print("=" * 60)

        # With 3 nodes and 1000 keys, ideal distribution is ~333 keys per node
        # Allow 20% variance (so between 200 and 467 keys per node)
        expected_per_node = total_successful / 3
        min_acceptable = expected_per_node * 0.5  # 50% of expected
        max_acceptable = expected_per_node * 1.5  # 150% of expected

        print(f"\nExpected keys per node: ~{expected_per_node:.0f}")
        print(f"Acceptable range: {min_acceptable:.0f} - {max_acceptable:.0f}")

        distribution_ok = True
        for node, count in node_counts.items():
            if count < min_acceptable:
                print(f"  ❌ {node} has too few keys: {count} (min: {min_acceptable:.0f})")
                distribution_ok = False
            elif count > max_acceptable:
                print(f"  ❌ {node} has too many keys: {count} (max: {max_acceptable:.0f})")
                distribution_ok = False
            else:
                print(f"  ✓ {node}: {count} keys (within acceptable range)")

        # Also check that all 3 nodes received some keys
        assert len(node_counts) == 3, f"Expected 3 nodes to receive keys, but got {len(node_counts)}"
        print(f"\n✓ All 3 nodes received keys")

        assert distribution_ok, "Key distribution is not even!"
        print("✓ Keys are evenly distributed across all nodes")

        # Calculate and display standard deviation
        counts = list(node_counts.values())
        mean = sum(counts) / len(counts)
        variance = sum((x - mean) ** 2 for x in counts) / len(counts)
        std_dev = variance ** 0.5
        coefficient_of_variation = (std_dev / mean) * 100

        print(f"\nStatistics:")
        print(f"  Mean: {mean:.1f}")
        print(f"  Std Dev: {std_dev:.1f}")
        print(f"  Coefficient of Variation: {coefficient_of_variation:.1f}%")

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
        print("\nKey distribution test successful:")
        print(f"  • Sent {NUM_KEYS} requests to a 3-node cluster")
        print(f"  • Keys are distributed across all nodes")
        print(f"  • Distribution is within acceptable variance")

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
    test_key_distribution()
